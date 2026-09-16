#!/usr/bin/env python3
import os
import sys
import json
import asyncio
import logging
import redis.asyncio as redis
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ConversationHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)
from datetime import datetime
from pathlib import Path
import uuid
import time
from functools import wraps

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

WAITING_FOR_FILE, WAITING_FOR_DATE, WAITING_FOR_LIMIT, WAITING_FOR_REPARSE_FILE = range(4)

REDIS_HOST = os.getenv('REDIS_HOST', 'redis')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
BOT_TOKEN = os.getenv('US_TELEGRAM_BOT_TOKEN')
TELEGRAM_PROXY = os.getenv('TELEGRAM_PROXY')
ALLOWED_USER_IDS_STR = os.getenv('US_ALLOWED_USER_IDS', '')
JOBS_STREAM = 'us_parser:jobs'
RESULTS_STREAM = 'us_parser:results'
PROGRESS_CHANNEL = 'us_parser:progress'
PROGRESS_EDIT_INTERVAL = 1.5
PROGRESS_BAR_WIDTH = 20
CANCEL_KEY_PREFIX = 'us_parser:cancel:'
CONSUMER_GROUP = 'us-bot-service'
JOB_LOCK_KEY = 'us_parser:job_lock'
JOB_LOCK_TTL = 7200
COOLDOWN_KEY = 'us_parser:cooldown'
COOLDOWN_SECONDS = 600
BUSY_MESSAGE = 'Расчет уже запущен. Подождите его окончания для следующего запуска'

redis_client = None

ALLOWED_USER_IDS = set()
if ALLOWED_USER_IDS_STR:
    try:
        ALLOWED_USER_IDS = set(int(uid.strip()) for uid in ALLOWED_USER_IDS_STR.split(',') if uid.strip())
    except ValueError:
        logger.warning("Invalid US_ALLOWED_USER_IDS format. Bot will be accessible to everyone.")


def authorized_only(func):
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user_id = update.effective_user.id
        username = update.effective_user.username or "Unknown"

        if ALLOWED_USER_IDS and user_id not in ALLOWED_USER_IDS:
            logger.warning(f"Unauthorized access attempt by user {user_id} (@{username})")
            await update.message.reply_text(
                "🚫 У вас нет доступа к этому боту.\n\n"
                "Если вы считаете, что это ошибка, свяжитесь с администратором."
            )
            return

        return await func(update, context, *args, **kwargs)

    return wrapper


async def get_redis():
    global redis_client
    if redis_client is None:
        redis_client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            decode_responses=True
        )
    return redis_client


async def is_job_running() -> bool:
    r = await get_redis()
    return bool(await r.exists(JOB_LOCK_KEY))


async def acquire_job_lock(job_id: str) -> bool:
    r = await get_redis()
    return bool(await r.set(JOB_LOCK_KEY, job_id, nx=True, ex=JOB_LOCK_TTL))


async def release_job_lock(job_id: str | None) -> None:
    if not job_id:
        return
    r = await get_redis()
    current = await r.get(JOB_LOCK_KEY)
    if current == job_id:
        await r.delete(JOB_LOCK_KEY)


async def request_job_cancel(job_id: str) -> None:
    r = await get_redis()
    await r.set(f'{CANCEL_KEY_PREFIX}{job_id}', '1', ex=JOB_LOCK_TTL)


async def clear_job_cancel(job_id: str | None) -> None:
    if not job_id:
        return
    r = await get_redis()
    await r.delete(f'{CANCEL_KEY_PREFIX}{job_id}')


async def set_parse_cooldown() -> None:
    r = await get_redis()
    await r.set(COOLDOWN_KEY, '1', ex=COOLDOWN_SECONDS)


async def get_cooldown_remaining() -> int:
    r = await get_redis()
    ttl = await r.ttl(COOLDOWN_KEY)
    return ttl if ttl and ttl > 0 else 0


def cooldown_message(remaining_seconds: int) -> str:
    minutes = max(1, (remaining_seconds + 59) // 60)
    return f'Следующий запуск будет доступен через {minutes} мин.'


def progress_bar(current: int, total: int, width: int = PROGRESS_BAR_WIDTH) -> str:
    if total <= 0:
        filled = 0
    else:
        filled = min(width, round(width * current / total))
    return '█' * filled + '░' * (width - filled)


def format_progress_text(
    filename: str,
    *,
    date_str: str | None = None,
    limit_text: str | None = None,
    reparse: bool = False,
    current: int | None = None,
    total: int | None = None,
) -> str:
    lines = ['🚀 Обработка файла...']
    if filename:
        lines.append(f'\n📊 Файл: {filename}')
    if date_str:
        lines.append(f'📅 Дата расчета: {date_str}')
    if limit_text:
        lines.append(f'📋 Лимит: {limit_text}')
    if reparse:
        lines.append('🔄 Режим: только строки с ERROR')

    lines.append('')
    if current is None or total is None:
        lines.append('⏳ Подготовка...')
    elif total <= 0:
        lines.append('⏳ Нет строк для обработки')
    else:
        percent = min(100, int(current * 100 / total))
        lines.append(f'⏳ {current}/{total} ({percent}%) {progress_bar(current, total)}')
    return '\n'.join(lines)


def format_result_text(title: str, date_str: str | None, body: str = '') -> str:
    parts = [title]
    if date_str:
        parts.append(f'📅 Дата расчета: {date_str}')
    if body:
        parts.append(body)
    return '\n\n'.join(parts)


def cancel_job_markup(job_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton('Отменить расчет', callback_data=f'cancel_job:{job_id}')
    ]])


EMPTY_INLINE_KEYBOARD = InlineKeyboardMarkup([])


def progress_store(application: Application) -> dict:
    return application.bot_data.setdefault('progress_messages', {})


def progress_lock(application: Application) -> asyncio.Lock:
    lock = application.bot_data.get('progress_lock')
    if lock is None:
        lock = asyncio.Lock()
        application.bot_data['progress_lock'] = lock
    return lock


async def start_progress_message(application: Application, chat_id: int, job_id: str, existing_message=None, **meta) -> None:
    text = format_progress_text(
        meta.get('filename', ''),
        date_str=meta.get('date_str'),
        limit_text=meta.get('limit_text'),
        reparse=meta.get('reparse', False),
    )
    markup = cancel_job_markup(job_id)
    sent = None
    if existing_message:
        try:
            await existing_message.edit_text(text, reply_markup=markup)
            sent = existing_message
        except Exception:
            sent = None
    if sent is None:
        sent = await application.bot.send_message(chat_id=chat_id, text=text, reply_markup=markup)
    progress_store(application)[job_id] = {
        'chat_id': sent.chat_id,
        'message_id': sent.message_id,
        'filename': meta.get('filename', ''),
        'date_str': meta.get('date_str'),
        'limit_text': meta.get('limit_text'),
        'reparse': meta.get('reparse', False),
        'last_edit': 0.0,
        'last_text': text,
    }


async def apply_progress_update(application: Application, data: dict) -> None:
    job_id = data.get('job_id')
    async with progress_lock(application):
        info = progress_store(application).get(job_id)
        if not info or info.get('cancelling'):
            return

        date_changed = False
        if data.get('date') and data.get('date') != info.get('date_str'):
            info['date_str'] = data['date']
            date_changed = True
        if 'current' in data:
            info['current'] = int(data['current'])
        if 'total' in data:
            info['total'] = int(data['total'])

        current = info.get('current')
        total = info.get('total')
        now = time.monotonic()
        is_final = total is not None and total > 0 and current is not None and current >= total
        if not is_final and not date_changed and now - info.get('last_edit', 0) < PROGRESS_EDIT_INTERVAL:
            return

        text = format_progress_text(
            info.get('filename', ''),
            date_str=info.get('date_str'),
            limit_text=info.get('limit_text'),
            reparse=info.get('reparse', False),
            current=current,
            total=total,
        )
        if text == info.get('last_text'):
            return

        try:
            await application.bot.edit_message_text(
                chat_id=info['chat_id'],
                message_id=info['message_id'],
                text=text,
                reply_markup=cancel_job_markup(job_id),
            )
            info['last_edit'] = now
            info['last_text'] = text
        except Exception:
            pass


async def finish_progress_message(application: Application, job_id: str | None, user_id: int, text: str) -> None:
    async with progress_lock(application):
        info = progress_store(application).pop(job_id, None) if job_id else None
        if info:
            try:
                await application.bot.edit_message_text(
                    chat_id=info['chat_id'],
                    message_id=info['message_id'],
                    text=text,
                    reply_markup=EMPTY_INLINE_KEYBOARD,
                )
                return
            except Exception:
                pass
    await application.bot.send_message(chat_id=user_id, text=text)


async def delete_busy_notice(application: Application) -> None:
    notice = application.bot_data.pop('busy_notice', None)
    if not notice:
        return
    try:
        await application.bot.delete_message(
            chat_id=notice['chat_id'],
            message_id=notice['message_id'],
        )
    except Exception:
        pass


async def notify_status(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str) -> None:
    await delete_busy_notice(context.application)

    query = update.callback_query
    if query and query.message:
        try:
            await query.edit_message_text(text)
            context.application.bot_data['busy_notice'] = {
                'chat_id': query.message.chat_id,
                'message_id': query.message.message_id,
            }
            return
        except Exception:
            pass

    msg = update.effective_message
    sent = await msg.reply_text(text)
    context.application.bot_data['busy_notice'] = {
        'chat_id': sent.chat_id,
        'message_id': sent.message_id,
    }


async def reject_if_busy(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if await is_job_running():
        await notify_status(update, context, BUSY_MESSAGE)
        return True
    remaining = await get_cooldown_remaining()
    if remaining > 0:
        await notify_status(update, context, cooldown_message(remaining))
        return True
    await delete_busy_notice(context.application)
    return False


@authorized_only
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 Добро пожаловать в бот парсинга цен акций (американские биржи)!\n\n"
        "Отправьте мне Excel файл (шаблон котировок) и я загружу актуальные цены акций с Investing.com.\n\n"
        "Команды:\n"
        "/parse - Начать обработку нового файла\n"
        "/reparse - Обработать только строки с ERROR в столбце E\n"
        "/cancel - Отменить текущую операцию\n"
        "/help - Показать справку"
    )


@authorized_only
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📖 Как использовать бот:\n\n"
        "Полная обработка (/parse):\n"
        "1. Отправьте команду /parse\n"
        "2. Загрузите Excel файл (шаблон котировок)\n"
        "3. Введите дату в формате ДД.ММ.ГГГГ (например, 31.10.2025)\n"
        "4. Дождитесь обработки (это может занять несколько минут)\n"
        "5. Получите заполненный Excel файл\n\n"
        "Формат столбцов:\n"
        "  E - Цена в USD (Investing.com)\n"
        "  F - Курс USD/RUB (ЦБ РФ)\n"
        "  G - Цена в RUB (E × F)\n\n"
        "Повторная обработка ошибок (/reparse):\n"
        "1. Отправьте команду /reparse\n"
        "2. Загрузите Excel файл со строками с ERROR в столбце E\n"
        "3. Бот повторно обработает только строки с ошибками\n\n"
        "Используйте /cancel для отмены текущей операции."
    )


@authorized_only
async def parse_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await reject_if_busy(update, context):
        return ConversationHandler.END

    user = update.effective_user
    logger.info(f"User {user.id} (@{user.username or 'unknown'}) invoked /parse")
    await update.message.reply_text(
        "📁 Пожалуйста, отправьте мне Excel файл (шаблон котировок) для обработки."
    )
    return WAITING_FOR_FILE


async def file_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await reject_if_busy(update, context):
        file_path = context.user_data.get('file_path')
        if file_path:
            Path(file_path).unlink(missing_ok=True)
        context.user_data.clear()
        return ConversationHandler.END

    document = update.message.document

    if not document.file_name.endswith(('.xlsx', '.xls')):
        await update.message.reply_text(
            "❌ Пожалуйста, отправьте Excel файл (.xlsx или .xls)"
        )
        return WAITING_FOR_FILE

    await update.message.reply_text("⏳ Загружаю файл...")

    file = await document.get_file()
    file_path = Path(f"/tmp/{uuid.uuid4()}_{document.file_name}")
    await file.download_to_drive(file_path)

    context.user_data['file_path'] = str(file_path)
    context.user_data['original_filename'] = document.file_name

    await update.message.reply_text(
        "✅ Файл получен!\n\n"
        "📅 Теперь введите дату для цен акций в формате ДД.ММ.ГГГГ\n"
        "Пример: 31.10.2025"
    )

    return WAITING_FOR_DATE


async def date_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await reject_if_busy(update, context):
        file_path = context.user_data.get('file_path')
        if file_path:
            Path(file_path).unlink(missing_ok=True)
        context.user_data.clear()
        return ConversationHandler.END

    date_str = update.message.text.strip()

    try:
        datetime.strptime(date_str, '%d.%m.%Y')
    except ValueError:
        await update.message.reply_text(
            "❌ Неверный формат даты. Пожалуйста, используйте формат ДД.ММ.ГГГГ (например, 31.10.2025)"
        )
        return WAITING_FOR_DATE

    file_path = context.user_data.get('file_path')

    if not file_path or not Path(file_path).exists():
        await update.message.reply_text(
            "❌ Файл не найден. Пожалуйста, начните заново с команды /parse"
        )
        return ConversationHandler.END

    context.user_data['date_str'] = date_str

    keyboard = [[InlineKeyboardButton("Парсить все", callback_data="parse_all")]]
    await update.message.reply_text(
        "📋 Введите количество строк для обработки (например: 10)\n"
        "Или нажмите кнопку для обработки всех строк.",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    return WAITING_FOR_LIMIT


async def limit_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await reject_if_busy(update, context):
        file_path = context.user_data.get('file_path')
        if file_path:
            Path(file_path).unlink(missing_ok=True)
        context.user_data.clear()
        return ConversationHandler.END

    text = update.message.text.strip()
    try:
        limit = int(text)
        if limit <= 0:
            raise ValueError("Limit must be positive")
    except ValueError:
        await update.message.reply_text(
            "❌ Введите целое положительное число или нажмите кнопку «Парсить все»."
        )
        return WAITING_FOR_LIMIT

    await _send_parse_job(update, context, limit=limit)
    return ConversationHandler.END


async def parse_all_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    started = await _send_parse_job(update, context, limit=None)
    if started:
        try:
            await query.message.delete()
        except Exception:
            try:
                await query.edit_message_text("✅ Выбрано: парсить все строки")
            except Exception:
                pass
    return ConversationHandler.END


async def _send_parse_job(update: Update, context: ContextTypes.DEFAULT_TYPE, limit: int | None) -> bool:
    file_path = context.user_data.get('file_path')
    original_filename = context.user_data.get('original_filename')
    date_str = context.user_data.get('date_str')
    user_id = update.effective_user.id

    msg = update.message or update.callback_query.message

    if not file_path or not Path(file_path).exists():
        await msg.reply_text(
            "❌ Файл не найден. Пожалуйста, начните заново с команды /parse"
        )
        return False

    if await reject_if_busy(update, context):
        Path(file_path).unlink(missing_ok=True)
        context.user_data.clear()
        return False

    job_id = str(uuid.uuid4())

    if not await acquire_job_lock(job_id):
        await notify_status(update, context, BUSY_MESSAGE)
        Path(file_path).unlink(missing_ok=True)
        context.user_data.clear()
        return False

    try:
        with open(file_path, 'rb') as f:
            file_content = f.read()

        r = await get_redis()

        job_data = {
            'job_id': job_id,
            'user_id': str(user_id),
            'filename': original_filename,
            'date': date_str,
            'file_content': file_content.hex(),
        }
        if limit is not None:
            job_data['limit'] = str(limit)

        await r.xadd(JOBS_STREAM, job_data)
    except Exception:
        await release_job_lock(job_id)
        raise

    username = update.effective_user.username or "unknown"
    limit_text = f"первые {limit} строк" if limit is not None else "все строки"
    logger.info(
        f"User {user_id} (@{username}) started parse: file={original_filename}, "
        f"date={date_str}, limit={limit_text}, job_id={job_id}"
    )
    await start_progress_message(
        context.application,
        update.effective_chat.id,
        job_id,
        filename=original_filename,
        date_str=date_str,
        limit_text=limit_text,
    )

    context.user_data['job_id'] = job_id
    context.user_data['chat_id'] = update.effective_chat.id

    Path(file_path).unlink(missing_ok=True)
    return True


@authorized_only
async def reparse_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await reject_if_busy(update, context):
        return ConversationHandler.END

    user = update.effective_user
    logger.info(f"User {user.id} (@{user.username or 'unknown'}) invoked /reparse")
    await update.message.reply_text(
        "📁 Пожалуйста, отправьте мне Excel файл с ERROR в столбце E для повторной обработки."
    )
    return WAITING_FOR_REPARSE_FILE


async def reparse_file_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await reject_if_busy(update, context):
        return ConversationHandler.END

    document = update.message.document

    if not document.file_name.endswith(('.xlsx', '.xls')):
        await update.message.reply_text(
            "❌ Пожалуйста, отправьте Excel файл (.xlsx или .xls)"
        )
        return WAITING_FOR_REPARSE_FILE

    loading_msg = await update.message.reply_text("⏳ Загружаю файл...")

    file = await document.get_file()
    file_path = Path(f"/tmp/{uuid.uuid4()}_{document.file_name}")
    await file.download_to_drive(file_path)

    job_id = str(uuid.uuid4())
    user_id = update.effective_user.id

    if not await acquire_job_lock(job_id):
        await notify_status(update, context, BUSY_MESSAGE)
        Path(file_path).unlink(missing_ok=True)
        try:
            await loading_msg.delete()
        except Exception:
            pass
        return ConversationHandler.END

    try:
        with open(file_path, 'rb') as f:
            file_content = f.read()

        r = await get_redis()

        job_data = {
            'job_id': job_id,
            'user_id': str(user_id),
            'filename': document.file_name,
            'file_content': file_content.hex(),
            'mode': 'reparse',
        }

        await r.xadd(JOBS_STREAM, job_data)
    except Exception:
        await release_job_lock(job_id)
        Path(file_path).unlink(missing_ok=True)
        raise

    username = update.effective_user.username or "unknown"
    logger.info(
        f"User {user_id} (@{username}) started reparse: file={document.file_name}, job_id={job_id}"
    )

    await start_progress_message(
        context.application,
        update.effective_chat.id,
        job_id,
        existing_message=loading_msg,
        filename=document.file_name,
        reparse=True,
    )

    Path(file_path).unlink(missing_ok=True)

    return ConversationHandler.END


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    file_path = context.user_data.get('file_path')
    if file_path:
        Path(file_path).unlink(missing_ok=True)

    context.user_data.clear()

    await update.message.reply_text(
        "❌ Операция отменена.",
        reply_markup=ReplyKeyboardRemove()
    )

    return ConversationHandler.END


async def cancel_job_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query or not query.data:
        return

    user_id = update.effective_user.id if update.effective_user else None
    if ALLOWED_USER_IDS and user_id not in ALLOWED_USER_IDS:
        await query.answer('Нет доступа', show_alert=True)
        return

    job_id = query.data.split(':', 1)[1]
    r = await get_redis()
    current = await r.get(JOB_LOCK_KEY)
    if current != job_id:
        await query.answer('Расчет уже завершен')
        try:
            await query.edit_message_reply_markup(reply_markup=EMPTY_INLINE_KEYBOARD)
        except Exception:
            pass
        return

    await query.answer('Отменяю расчет...')
    await request_job_cancel(job_id)

    async with progress_lock(context.application):
        info = progress_store(context.application).get(job_id)
        if not info:
            return
        info['cancelling'] = True
        text = format_result_text('⏳ Отменяю расчет...', info.get('date_str'))
        try:
            await query.edit_message_text(text, reply_markup=EMPTY_INLINE_KEYBOARD)
            info['last_text'] = text
        except Exception:
            pass


async def listen_for_progress(application: Application):
    r = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True
    )
    pubsub = r.pubsub()
    await pubsub.subscribe(PROGRESS_CHANNEL)
    logger.info(f"Listening for progress on channel: {PROGRESS_CHANNEL}")
    try:
        async for message in pubsub.listen():
            if message.get('type') != 'message':
                continue
            try:
                data = json.loads(message['data'])
                await apply_progress_update(application, data)
            except Exception as e:
                logger.error(f"Error processing progress: {e}")
    finally:
        await pubsub.unsubscribe(PROGRESS_CHANNEL)
        await r.aclose()


async def listen_for_results(application: Application):
    r = await get_redis()

    try:
        await r.xgroup_create(RESULTS_STREAM, CONSUMER_GROUP, id='0', mkstream=True)
    except redis.ResponseError:
        pass

    logger.info(f"Listening for results on stream: {RESULTS_STREAM}")

    while True:
        try:
            messages = await r.xreadgroup(
                CONSUMER_GROUP,
                'bot-consumer',
                {RESULTS_STREAM: '>'},
                count=10,
                block=1000
            )

            for stream, stream_messages in messages:
                for message_id, data in stream_messages:
                    try:
                        await process_result(application, data)
                        await r.xack(RESULTS_STREAM, CONSUMER_GROUP, message_id)
                    except Exception as e:
                        logger.error(f"Error processing result: {e}")

        except Exception as e:
            logger.error(f"Error reading from Redis stream: {e}")
            await asyncio.sleep(5)


async def process_result(application: Application, data: dict):
    job_id = data.get('job_id')
    user_id = int(data.get('user_id'))
    status = data.get('status')
    stored = progress_store(application).get(job_id) or {}
    date_str = data.get('date') or stored.get('date_str')

    try:
        await delete_busy_notice(application)

        if status == 'success':
            file_content = bytes.fromhex(data.get('file_content'))
            filename = data.get('filename')
            summary = data.get('summary', '')

            output_filename = filename.replace('.xlsx', '_filled.xlsx')

            await finish_progress_message(
                application,
                job_id,
                user_id,
                format_result_text('✅ Обработка завершена!', date_str, summary)
            )

            await application.bot.send_document(
                chat_id=user_id,
                document=file_content,
                filename=output_filename,
                caption="Вот ваш обработанный файл 📊"
            )
            logger.info(f"User {user_id} successfully received result for job {job_id} ({filename})")

        elif status == 'error':
            error_message = data.get('error', 'Unknown error')

            await finish_progress_message(
                application,
                job_id,
                user_id,
                format_result_text('❌ Обработка не удалась!', date_str, f'Ошибка: {error_message}')
            )
            logger.error(f"User {user_id} received error for job {job_id}: {error_message}")

        elif status == 'cancelled':
            await finish_progress_message(
                application,
                job_id,
                user_id,
                format_result_text('❌ Расчет отменен', date_str)
            )
            logger.info(f"User {user_id} cancelled job {job_id}")
    finally:
        await release_job_lock(job_id)
        await clear_job_cancel(job_id)
        if status != 'cancelled':
            await set_parse_cooldown()


async def post_init(application: Application):
    asyncio.create_task(listen_for_results(application))
    asyncio.create_task(listen_for_progress(application))


def main():
    if not BOT_TOKEN:
        logger.error("US_TELEGRAM_BOT_TOKEN environment variable not set")
        return

    if ALLOWED_USER_IDS:
        logger.info(f"Bot is restricted to {len(ALLOWED_USER_IDS)} authorized user(s)")
    else:
        logger.warning("Bot is accessible to EVERYONE. Set US_ALLOWED_USER_IDS to restrict access.")

    builder = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .connect_timeout(30.0)
        .read_timeout(30.0)
        .write_timeout(30.0)
        .pool_timeout(30.0)
    )
    if TELEGRAM_PROXY:
        builder = builder.proxy(TELEGRAM_PROXY).get_updates_proxy(TELEGRAM_PROXY)
    application = builder.build()

    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('parse', parse_command)],
        states={
            WAITING_FOR_FILE: [
                MessageHandler(filters.Document.ALL, file_received)
            ],
            WAITING_FOR_DATE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, date_received)
            ],
            WAITING_FOR_LIMIT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, limit_received),
                CallbackQueryHandler(parse_all_callback, pattern="^parse_all$"),
            ],
        },
        fallbacks=[CommandHandler('cancel', cancel)],
    )

    reparse_conv_handler = ConversationHandler(
        entry_points=[CommandHandler('reparse', reparse_command)],
        states={
            WAITING_FOR_REPARSE_FILE: [
                MessageHandler(filters.Document.ALL, reparse_file_received)
            ],
        },
        fallbacks=[CommandHandler('cancel', cancel)],
    )

    application.add_handler(CommandHandler('start', start))
    application.add_handler(CommandHandler('help', help_command))
    application.add_handler(CallbackQueryHandler(cancel_job_callback, pattern=r'^cancel_job:'))
    application.add_handler(conv_handler)
    application.add_handler(reparse_conv_handler)

    logger.info("US Bot started!")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == '__main__':
    main()

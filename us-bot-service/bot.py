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
from functools import wraps

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

WAITING_FOR_FILE, WAITING_FOR_DATE, WAITING_FOR_LIMIT, WAITING_FOR_REPARSE_FILE = range(4)

REDIS_HOST = os.getenv('REDIS_HOST', 'redis')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
BOT_TOKEN = os.getenv('US_TELEGRAM_BOT_TOKEN')
ALLOWED_USER_IDS_STR = os.getenv('US_ALLOWED_USER_IDS', '')
JOBS_STREAM = 'us_parser:jobs'
RESULTS_STREAM = 'us_parser:results'
CONSUMER_GROUP = 'us-bot-service'

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
    await update.message.reply_text(
        "📁 Пожалуйста, отправьте мне Excel файл (шаблон котировок) для обработки."
    )
    return WAITING_FOR_FILE


async def file_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
    await query.edit_message_text("✅ Выбрано: парсить все строки")
    await _send_parse_job(update, context, limit=None)
    return ConversationHandler.END


async def _send_parse_job(update: Update, context: ContextTypes.DEFAULT_TYPE, limit: int | None):
    file_path = context.user_data.get('file_path')
    original_filename = context.user_data.get('original_filename')
    date_str = context.user_data.get('date_str')
    user_id = update.effective_user.id

    msg = update.message or update.callback_query.message

    if not file_path or not Path(file_path).exists():
        await msg.reply_text(
            "❌ Файл не найден. Пожалуйста, начните заново с команды /parse"
        )
        return

    job_id = str(uuid.uuid4())

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

    limit_text = f"первые {limit} строк" if limit is not None else "все строки"
    await msg.reply_text(
        f"🚀 Обработка начата!\n\n"
        f"📊 Файл: {original_filename}\n"
        f"📅 Дата: {date_str}\n"
        f"📋 Лимит: {limit_text}\n\n"
        f"⏳ Это может занять несколько минут. Я отправлю вам результат, когда он будет готов.\n\n"
        f"ID задачи: {job_id}"
    )

    context.user_data['job_id'] = job_id
    context.user_data['chat_id'] = update.effective_chat.id

    Path(file_path).unlink(missing_ok=True)


@authorized_only
async def reparse_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📁 Пожалуйста, отправьте мне Excel файл с ERROR в столбце E для повторной обработки."
    )
    return WAITING_FOR_REPARSE_FILE


async def reparse_file_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    document = update.message.document

    if not document.file_name.endswith(('.xlsx', '.xls')):
        await update.message.reply_text(
            "❌ Пожалуйста, отправьте Excel файл (.xlsx или .xls)"
        )
        return WAITING_FOR_REPARSE_FILE

    await update.message.reply_text("⏳ Загружаю файл...")

    file = await document.get_file()
    file_path = Path(f"/tmp/{uuid.uuid4()}_{document.file_name}")
    await file.download_to_drive(file_path)

    with open(file_path, 'rb') as f:
        file_content = f.read()

    job_id = str(uuid.uuid4())
    user_id = update.effective_user.id

    r = await get_redis()

    job_data = {
        'job_id': job_id,
        'user_id': str(user_id),
        'filename': document.file_name,
        'file_content': file_content.hex(),
        'mode': 'reparse',
    }

    await r.xadd(JOBS_STREAM, job_data)

    await update.message.reply_text(
        f"🚀 Повторная обработка начата!\n\n"
        f"📊 Файл: {document.file_name}\n"
        f"🔄 Режим: только строки с ERROR в столбце E\n\n"
        f"⏳ Это может занять несколько минут. Я отправлю вам результат, когда он будет готов.\n\n"
        f"ID задачи: {job_id}"
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

    logger.info(f"Received result for job {job_id}: {status}")

    if status == 'success':
        file_content = bytes.fromhex(data.get('file_content'))
        filename = data.get('filename')
        summary = data.get('summary', '')

        output_filename = filename.replace('.xlsx', '_filled.xlsx')

        await application.bot.send_message(
            chat_id=user_id,
            text=f"✅ Обработка завершена!\n\n{summary}"
        )

        await application.bot.send_document(
            chat_id=user_id,
            document=file_content,
            filename=output_filename,
            caption="Вот ваш обработанный файл 📊"
        )

    elif status == 'error':
        error_message = data.get('error', 'Unknown error')

        await application.bot.send_message(
            chat_id=user_id,
            text=f"❌ Обработка не удалась!\n\nОшибка: {error_message}"
        )


async def post_init(application: Application):
    asyncio.create_task(listen_for_results(application))


def main():
    if not BOT_TOKEN:
        logger.error("US_TELEGRAM_BOT_TOKEN environment variable not set")
        return

    if ALLOWED_USER_IDS:
        logger.info(f"Bot is restricted to {len(ALLOWED_USER_IDS)} authorized user(s)")
    else:
        logger.warning("Bot is accessible to EVERYONE. Set US_ALLOWED_USER_IDS to restrict access.")

    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .connect_timeout(30.0)
        .read_timeout(30.0)
        .write_timeout(30.0)
        .pool_timeout(30.0)
        .build()
    )

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
    application.add_handler(conv_handler)
    application.add_handler(reparse_conv_handler)

    logger.info("US Bot started!")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == '__main__':
    main()

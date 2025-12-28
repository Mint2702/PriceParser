#!/usr/bin/env python3
import os
import json
import asyncio
import redis.asyncio as redis
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ConversationHandler,
    ContextTypes,
    filters,
)
from datetime import datetime
from pathlib import Path
import uuid
from functools import wraps

WAITING_FOR_FILE, WAITING_FOR_DATE = range(2)

REDIS_HOST = os.getenv('REDIS_HOST', 'redis')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
ALLOWED_USER_IDS_STR = os.getenv('ALLOWED_USER_IDS', '')
JOBS_STREAM = 'parser:jobs'
RESULTS_STREAM = 'parser:results'
CONSUMER_GROUP = 'bot-service'

redis_client = None

ALLOWED_USER_IDS = set()
if ALLOWED_USER_IDS_STR:
    try:
        ALLOWED_USER_IDS = set(int(uid.strip()) for uid in ALLOWED_USER_IDS_STR.split(',') if uid.strip())
    except ValueError:
        print("⚠️  Warning: Invalid ALLOWED_USER_IDS format. Bot will be accessible to everyone.")


def authorized_only(func):
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user_id = update.effective_user.id
        username = update.effective_user.username or "Unknown"
        
        if ALLOWED_USER_IDS and user_id not in ALLOWED_USER_IDS:
            print(f"⛔ Unauthorized access attempt by user {user_id} (@{username})")
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
        "👋 Добро пожаловать в бот парсинга цен акций!\n\n"
        "Отправьте мне Excel файл (шаблон котировок) и я загружу актуальные цены акций.\n\n"
        "Команды:\n"
        "/parse - Начать обработку нового файла\n"
        "/cancel - Отменить текущую операцию\n"
        "/help - Показать справку"
    )


@authorized_only
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📖 Как использовать бот:\n\n"
        "1. Отправьте команду /parse\n"
        "2. Загрузите Excel файл (шаблон котировок)\n"
        "3. Введите дату в формате ДД.ММ.ГГГГ (например, 31.10.2025)\n"
        "4. Дождитесь обработки (это может занять несколько минут)\n"
        "5. Получите заполненный Excel файл\n\n"
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
        date = datetime.strptime(date_str, '%d.%m.%Y')
    except ValueError:
        await update.message.reply_text(
            "❌ Неверный формат даты. Пожалуйста, используйте формат ДД.ММ.ГГГГ (например, 31.10.2025)"
        )
        return WAITING_FOR_DATE
    
    file_path = context.user_data.get('file_path')
    original_filename = context.user_data.get('original_filename')
    
    if not file_path or not Path(file_path).exists():
        await update.message.reply_text(
            "❌ Файл не найден. Пожалуйста, начните заново с команды /parse"
        )
        return ConversationHandler.END
    
    job_id = str(uuid.uuid4())
    user_id = update.effective_user.id
    
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
    
    await r.xadd(JOBS_STREAM, job_data)
    
    await update.message.reply_text(
        f"🚀 Обработка начата!\n\n"
        f"📊 Файл: {original_filename}\n"
        f"📅 Дата: {date_str}\n\n"
        f"⏳ Это может занять несколько минут. Я отправлю вам результат, когда он будет готов.\n\n"
        f"ID задачи: {job_id}"
    )
    
    context.user_data['job_id'] = job_id
    context.user_data['chat_id'] = update.effective_chat.id
    
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
    
    print(f"👂 Listening for results on stream: {RESULTS_STREAM}")
    
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
                        print(f"Error processing result: {e}")
        
        except Exception as e:
            print(f"Error reading from Redis stream: {e}")
            await asyncio.sleep(5)


async def process_result(application: Application, data: dict):
    job_id = data.get('job_id')
    user_id = int(data.get('user_id'))
    status = data.get('status')
    
    print(f"📨 Received result for job {job_id}: {status}")
    
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
        print("Error: TELEGRAM_BOT_TOKEN environment variable not set")
        return
    
    if ALLOWED_USER_IDS:
        print(f"🔒 Bot is restricted to {len(ALLOWED_USER_IDS)} authorized user(s)")
    else:
        print("⚠️  WARNING: Bot is accessible to EVERYONE. Set ALLOWED_USER_IDS to restrict access.")
    
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
        },
        fallbacks=[CommandHandler('cancel', cancel)],
    )
    
    application.add_handler(CommandHandler('start', start))
    application.add_handler(CommandHandler('help', help_command))
    application.add_handler(conv_handler)
    
    print("🤖 Bot started!")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == '__main__':
    main()


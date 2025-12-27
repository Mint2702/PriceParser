# Stock Price Parser Telegram Bot

A microservices-based Telegram bot that fetches stock prices from MOEX and Investing.com and fills Excel quotations templates.

## Architecture

The system consists of three services running in Docker containers:

- **bot-service**: Telegram bot that handles user interactions
- **parser-service**: Worker that processes Excel files
- **redis**: Message broker for inter-service communication using Redis Streams

## Prerequisites

- Docker and Docker Compose installed
- Telegram Bot Token (get from [@BotFather](https://t.me/botfather))

## Setup

1. **Create your Telegram bot:**
   ```bash
   # Start a chat with @BotFather on Telegram
   # Send /newbot and follow the instructions
   # Copy the bot token you receive
   ```

2. **Configure environment variables:**
   ```bash
   cp .env.example .env
   # Edit .env and add your bot token:
   # TELEGRAM_BOT_TOKEN=your_token_here
   ```

3. **Build and start services:**
   ```bash
   docker-compose up --build
   ```

   To run in background:
   ```bash
   docker-compose up -d --build
   ```

4. **Check logs:**
   ```bash
   # All services
   docker-compose logs -f
   
   # Specific service
   docker-compose logs -f bot-service
   docker-compose logs -f parser-service
   ```

## Usage

1. Start a chat with your bot on Telegram
2. Send `/start` to see available commands
3. Send `/parse` to begin:
   - Upload your Excel file (quotations template)
   - Enter the date in DD.MM.YYYY format (e.g., 31.10.2025)
   - Wait for processing (usually 2-5 minutes)
   - Receive the filled Excel file

## Bot Commands

- `/start` - Welcome message and overview
- `/parse` - Start parsing a new file
- `/help` - Show usage instructions
- `/cancel` - Cancel current operation

## Configuration

### Environment Variables

**Bot Service:**
- `TELEGRAM_BOT_TOKEN` - Your Telegram bot token (required)
- `REDIS_HOST` - Redis hostname (default: redis)
- `REDIS_PORT` - Redis port (default: 6379)

**Parser Service:**
- `REDIS_HOST` - Redis hostname (default: redis)
- `REDIS_PORT` - Redis port (default: 6379)
- `BATCH_SIZE` - Number of concurrent requests per batch (default: 10)

### Scaling

To scale parser workers:
```bash
docker-compose up -d --scale parser-service=3
```

This will run 3 parser workers to process jobs faster.

## Management

**Stop services:**
```bash
docker-compose down
```

**Stop and remove volumes:**
```bash
docker-compose down -v
```

**Rebuild after code changes:**
```bash
docker-compose up --build
```

**View running containers:**
```bash
docker-compose ps
```

## Troubleshooting

**Bot not responding:**
1. Check if bot token is correct in `.env`
2. Check bot-service logs: `docker-compose logs bot-service`
3. Verify bot-service is running: `docker-compose ps`

**Parser not processing files:**
1. Check parser-service logs: `docker-compose logs parser-service`
2. Verify Redis is running: `docker-compose ps redis`
3. Check Redis connection: `docker-compose exec redis redis-cli ping`

**Files not being processed:**
1. Check both service logs
2. Verify Redis streams: `docker-compose exec redis redis-cli XINFO STREAM parser:jobs`

## Development

**Local development without Docker:**

1. Install dependencies:
   ```bash
   cd bot-service
   pip install -r requirements.txt
   
   cd ../parser-service
   pip install -r requirements.txt
   ```

2. Run Redis locally:
   ```bash
   docker run -d -p 6379:6379 redis:7-alpine
   ```

3. Run services:
   ```bash
   # Terminal 1
   cd bot-service
   export TELEGRAM_BOT_TOKEN=your_token
   export REDIS_HOST=localhost
   python bot.py
   
   # Terminal 2
   cd parser-service
   export REDIS_HOST=localhost
   python parser_worker.py
   ```

## Architecture Details

### Communication Flow

1. User sends file and date to Telegram bot
2. Bot uploads file content to Redis Stream (`parser:jobs`)
3. Parser worker reads from Redis Stream
4. Parser processes file using async price fetching
5. Parser sends result to Redis Stream (`parser:results`)
6. Bot reads result and sends filled file back to user

### Redis Streams

**Jobs Stream** (`parser:jobs`):
- `job_id`: Unique job identifier
- `user_id`: Telegram user ID
- `filename`: Original filename
- `date`: Target date for prices
- `file_content`: Excel file content (hex encoded)

**Results Stream** (`parser:results`):
- `job_id`: Job identifier
- `user_id`: Telegram user ID
- `status`: `success` or `error`
- `filename`: Output filename (for success)
- `file_content`: Processed file content (for success)
- `summary`: Processing summary (for success)
- `error`: Error message (for error)

## License

Same as the main project.


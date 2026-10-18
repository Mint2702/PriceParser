#!/bin/bash

echo "🚀 Starting Stock Price Parser Telegram Bot"
echo "=========================================="

if [ ! -f .env ]; then
    echo "⚠️  .env file not found!"
    echo "📝 Creating .env from .env.example..."
    cp .env.example .env
    echo ""
    echo "❗ IMPORTANT: Edit .env file and add your TELEGRAM_BOT_TOKEN"
    echo "   Get your token from @BotFather on Telegram"
    echo ""
    echo "   nano .env"
    echo ""
    read -p "Press Enter after you've added your bot token..."
fi

if ! grep -q "^TELEGRAM_BOT_TOKEN=.\+$" .env 2>/dev/null; then
    echo "❌ Error: TELEGRAM_BOT_TOKEN is not set in .env file"
    echo "   Please edit .env and add your bot token"
    exit 1
fi

echo "✅ Environment configured"
echo ""
echo "🛑 Stopping and removing old containers..."
docker compose down

echo ""
echo "🧹 Cleaning up old images and containers..."
docker compose rm -f

echo ""
echo "🐳 Building and starting Docker containers..."
docker compose up --build -d

echo ""
echo "⏳ Waiting for services to start..."
sleep 5

echo ""
echo "📊 Service Status:"
docker compose ps

echo ""
echo "✅ Bot is running!"
echo ""
echo "📱 Next steps:"
echo "   1. Open Telegram and find your bot"
echo "   2. Send /start to begin"
echo "   3. Use /parse to process Excel files"
echo ""
echo "📋 Useful commands:"
echo "   View logs:        docker compose logs -f"
echo "   Stop services:    docker compose down"
echo "   Restart:          docker compose restart"
echo ""


#!/bin/bash

echo "🔄 Updating Stock Price Parser Telegram Bot"
echo "=========================================="
echo ""

echo "📥 Pulling latest changes from git..."
git pull

if [ $? -ne 0 ]; then
    echo "❌ Git pull failed. Please resolve conflicts and try again."
    exit 1
fi

echo ""
echo "🛑 Stopping all containers..."
docker compose down

echo ""
echo "🧹 Removing old containers and images..."
docker compose rm -f
docker compose down --rmi local --volumes --remove-orphans

echo ""
echo "🐳 Rebuilding and starting containers with latest code..."
docker compose build --no-cache
docker compose up -d

echo ""
echo "⏳ Waiting for services to start..."
sleep 5

echo ""
echo "📊 Service Status:"
docker compose ps

echo ""
echo "✅ Update complete!"
echo ""
echo "📋 Useful commands:"
echo "   View logs:        docker compose logs -f"
echo "   View bot logs:    docker compose logs -f bot-service"
echo "   View parser logs: docker compose logs -f parser_service"
echo "   Stop services:    docker compose down"
echo ""

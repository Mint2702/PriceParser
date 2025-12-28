#!/bin/bash

echo "🛑 Stopping Stock Price Parser Telegram Bot"
echo "=========================================="
echo ""

echo "Stopping all containers..."
docker-compose down

echo ""
echo "✅ All services stopped!"
echo ""
echo "To start again, run: ./start.sh"
echo ""

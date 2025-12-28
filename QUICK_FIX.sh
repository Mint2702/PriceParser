#!/bin/bash
# Quick fix for network issues on your server

set -e

echo "========================================="
echo "PriceParser Bot Network Fix"
echo "========================================="
echo ""

# Navigate to project directory
cd "$(dirname "$0")"

echo "Step 1: Stopping containers..."
docker-compose down

echo ""
echo "Step 2: Rebuilding bot service..."
docker-compose build bot-service

echo ""
echo "Step 3: Starting all services..."
docker-compose up -d

echo ""
echo "Step 4: Waiting for services to initialize (15 seconds)..."
sleep 15

echo ""
echo "Step 5: Checking bot service logs..."
echo "========================================="
docker-compose logs --tail=30 bot-service

echo ""
echo "========================================="
echo "If you see '🤖 Bot started!' above, it's working!"
echo ""
echo "To continue monitoring:"
echo "  docker-compose logs -f bot-service"
echo ""
echo "To check all services:"
echo "  docker-compose ps"
echo "========================================="


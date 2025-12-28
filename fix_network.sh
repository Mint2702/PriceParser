#!/bin/bash

echo "=== Network Diagnostics ==="

echo "1. Testing internet connectivity..."
ping -c 4 8.8.8.8

echo -e "\n2. Testing DNS resolution..."
nslookup api.telegram.org

echo -e "\n3. Testing HTTPS connectivity to Telegram..."
curl -v https://api.telegram.org/bot

echo -e "\n4. Checking Docker containers status..."
docker ps -a

echo -e "\n5. Checking Docker network..."
docker network inspect priceparser-network

echo -e "\n=== Fixes ==="

echo "Restarting Docker services..."
cd "$(dirname "$0")"
docker-compose down
docker-compose up -d

echo -e "\nWaiting for services to start..."
sleep 10

echo -e "\nChecking logs..."
docker-compose logs --tail=50 bot-service

echo -e "\n=== Done ==="
echo "If you still see errors, check your server firewall:"
echo "  sudo iptables -L"
echo "  sudo ufw status"


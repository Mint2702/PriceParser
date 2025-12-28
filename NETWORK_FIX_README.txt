NETWORK ISSUES DIAGNOSIS AND FIXES
===================================

ISSUE #1: PostgreSQL "Interference"
------------------------------------
FALSE ALARM: Your docker-compose.yml does NOT have PostgreSQL.
Only Redis, bot-service, and parser-service are running in Docker.

If you have systemctl PostgreSQL running, it's not interfering with this project.

To check:
  sudo systemctl status postgresql

If you don't need it:
  sudo systemctl stop postgresql
  sudo systemctl disable postgresql


ISSUE #2: Bot Network Connection Error (REAL ISSUE)
---------------------------------------------------
The bot cannot connect to Telegram's API servers (api.telegram.org).

Root causes:
1. Docker container network isolation
2. DNS resolution issues
3. Firewall blocking outbound HTTPS
4. Server network connectivity problems

FIXES APPLIED:
--------------
1. Added DNS servers (8.8.8.8, 8.8.4.4) to bot-service in docker-compose.yml
2. Added connection timeout settings to bot.py for better resilience


STEPS TO FIX ON YOUR SERVER:
-----------------------------

1. Upload the updated files to your server:
   - docker-compose.yml
   - bot-service/bot.py

2. Run the diagnostic script:
   ./fix_network.sh

3. If issues persist, manually check:

   a) Test internet connectivity:
      ping -c 4 8.8.8.8

   b) Test DNS:
      nslookup api.telegram.org
      dig api.telegram.org

   c) Test HTTPS to Telegram:
      curl -v https://api.telegram.org/bot

   d) Check firewall:
      sudo iptables -L -n
      sudo ufw status

   e) Check Docker network:
      docker network ls
      docker network inspect priceparser-network

4. If firewall is blocking, allow outbound HTTPS:
   sudo ufw allow out 443/tcp
   sudo ufw allow out 53/tcp
   sudo ufw allow out 53/udp

5. Restart Docker services:
   cd /path/to/PriceParser
   docker-compose down
   docker-compose up -d

6. Monitor logs:
   docker-compose logs -f bot-service


ALTERNATIVE: Use Host Network Mode
----------------------------------
If the issue persists, you can run the bot with host network mode.
This gives the container direct access to the server's network.

In docker-compose.yml, change bot-service section:

  bot-service:
    network_mode: "host"  # Add this
    # Remove the networks section

WARNING: This is less secure but ensures network access.


CHECKING IF IT'S FIXED:
-----------------------
After restart, check logs:
  docker-compose logs -f bot-service

You should see:
  ✅ "🤖 Bot started!"
  ✅ "👂 Listening for results on stream: parser:results"
  ❌ NO "httpx.ConnectError" errors


ADDITIONAL TIPS:
---------------
1. Ensure your server has stable internet connection
2. Check if Telegram is accessible in your region
3. Consider using a VPN/proxy if Telegram is blocked
4. Monitor Redis connection: docker-compose logs redis


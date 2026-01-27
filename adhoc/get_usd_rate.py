#!/usr/bin/env python3
import sys
import asyncio
import logging
import xml.etree.ElementTree as ET
from datetime import datetime
from curl_cffi.requests import AsyncSession

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


async def get_usd_rate_from_cbr(date: datetime) -> float | None:
    try:
        date_str = date.strftime('%d/%m/%Y')
        url = f"https://cbr.ru/scripts/XML_daily.asp?date_req={date_str}"
        
        logger.info(f"Fetching USD rate from: {url}")
        
        async with AsyncSession() as client:
            response = await client.get(url, timeout=30, impersonate="chrome120")
            response.raise_for_status()
            
            root = ET.fromstring(response.content)
            
            logger.info(f"Date from CBR: {root.get('Date')}")
            logger.info("Searching for USD...")
            
            for valute in root.findall('Valute'):
                char_code = valute.find('CharCode')
                if char_code is not None and char_code.text == 'USD':
                    nominal = valute.find('Nominal')
                    name = valute.find('Name')
                    value = valute.find('Value')
                    
                    if value is not None and value.text:
                        usd_rate = float(value.text.replace(',', '.'))
                        
                        logger.info(f"Found USD:")
                        logger.info(f"  Name: {name.text if name is not None else 'N/A'}")
                        logger.info(f"  Nominal: {nominal.text if nominal is not None else 'N/A'}")
                        logger.info(f"  Value: {value.text} → {usd_rate}")
                        
                        return usd_rate
            
            logger.warning("USD not found in response")
            return None
    except Exception as e:
        logger.error(f"Error fetching USD rate from CBR: {e}")
        return None


async def main():
    if len(sys.argv) < 2:
        logger.info("Usage: python get_usd_rate.py <date>")
        logger.info("Date format: DD.MM.YYYY or YYYY-MM-DD")
        logger.info("\nExamples:")
        logger.info("  python get_usd_rate.py 26.12.2025")
        logger.info("  python get_usd_rate.py 2025-12-26")
        sys.exit(1)
    
    date_str = sys.argv[1]
    
    try:
        if '.' in date_str:
            date = datetime.strptime(date_str, '%d.%m.%Y')
        else:
            date = datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        logger.error(f"Invalid date format: {date_str}")
        logger.info("Use DD.MM.YYYY or YYYY-MM-DD format")
        sys.exit(1)
    
    logger.info(f"Requesting USD rate for: {date.strftime('%d.%m.%Y')}")
    logger.info("=" * 60)
    
    usd_rate = await get_usd_rate_from_cbr(date)
    
    logger.info("=" * 60)
    if usd_rate:
        logger.info(f"USD Exchange Rate: {usd_rate} RUB")
    else:
        logger.error("Failed to fetch USD rate")
        sys.exit(1)


if __name__ == '__main__':
    asyncio.run(main())

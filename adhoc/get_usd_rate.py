#!/usr/bin/env python3
import sys
import asyncio
import xml.etree.ElementTree as ET
from datetime import datetime
from curl_cffi.requests import AsyncSession


async def get_usd_rate_from_cbr(date: datetime) -> float | None:
    try:
        date_str = date.strftime('%d/%m/%Y')
        url = f"https://cbr.ru/scripts/XML_daily.asp?date_req={date_str}"
        
        print(f"Fetching USD rate from: {url}")
        
        async with AsyncSession() as client:
            response = await client.get(url, timeout=30, impersonate="chrome120")
            response.raise_for_status()
            
            root = ET.fromstring(response.content)
            
            print(f"\nDate from CBR: {root.get('Date')}")
            print("\nSearching for USD...")
            
            for valute in root.findall('Valute'):
                char_code = valute.find('CharCode')
                if char_code is not None and char_code.text == 'USD':
                    nominal = valute.find('Nominal')
                    name = valute.find('Name')
                    value = valute.find('Value')
                    
                    if value is not None and value.text:
                        usd_rate = float(value.text.replace(',', '.'))
                        
                        print(f"\n✅ Found USD:")
                        print(f"  Name: {name.text if name is not None else 'N/A'}")
                        print(f"  Nominal: {nominal.text if nominal is not None else 'N/A'}")
                        print(f"  Value: {value.text} → {usd_rate}")
                        
                        return usd_rate
            
            print("\n❌ USD not found in response")
            return None
    except Exception as e:
        print(f"\n❌ Error fetching USD rate from CBR: {e}")
        return None


async def main():
    if len(sys.argv) < 2:
        print("Usage: python get_usd_rate.py <date>")
        print("Date format: DD.MM.YYYY or YYYY-MM-DD")
        print("\nExamples:")
        print("  python get_usd_rate.py 26.12.2025")
        print("  python get_usd_rate.py 2025-12-26")
        sys.exit(1)
    
    date_str = sys.argv[1]
    
    try:
        if '.' in date_str:
            date = datetime.strptime(date_str, '%d.%m.%Y')
        else:
            date = datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        print(f"❌ Invalid date format: {date_str}")
        print("Use DD.MM.YYYY or YYYY-MM-DD format")
        sys.exit(1)
    
    print(f"📅 Requesting USD rate for: {date.strftime('%d.%m.%Y')}")
    print("=" * 60)
    
    usd_rate = await get_usd_rate_from_cbr(date)
    
    print("\n" + "=" * 60)
    if usd_rate:
        print(f"💵 USD Exchange Rate: {usd_rate} RUB")
    else:
        print("❌ Failed to fetch USD rate")
        sys.exit(1)


if __name__ == '__main__':
    asyncio.run(main())

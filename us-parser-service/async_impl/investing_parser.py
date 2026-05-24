import asyncio
import re
import logging
from curl_cffi.requests import AsyncSession
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


async def get_stock_id_async(stock_url: str) -> tuple[int, str | None]:
    max_retries = 3
    retry_delays = [2, 4, 8]
    
    for attempt in range(max_retries):
        try:
            async with AsyncSession() as client:
                response = await client.get(
                    stock_url, 
                    timeout=30, 
                    impersonate="chrome120", 
                    headers={"Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7", "Domain-Id": "ru"}
                )
                data = response.text

                soup = BeautifulSoup(data, "html.parser")
                script_data = soup.find("script", id="__NEXT_DATA__").text
                
                match = re.search(r'"identifiers"\s*:\s*\{[^}]*"instrument_id"\s*:\s*"?(\d+)"?', script_data)
                if match:
                    stock_id = int(match.group(1))
                else:
                    raise ValueError("instrument_id not found in identifiers object")

                currency_tag = soup.find(attrs={"data-test": "currency-in-label"})
                currency = currency_tag.get_text(strip=True).split()[-1][1:] if currency_tag else None

                return stock_id, currency
                
        except Exception as e:
            if attempt < max_retries - 1:
                delay = retry_delays[attempt]
                logger.warning(f"Investing get_stock_id error (attempt {attempt + 1}/{max_retries}): {e}. Retrying in {delay}s... Url: {stock_url}")
                await asyncio.sleep(delay)
            else:
                logger.error(f"Investing get_stock_id failed after {max_retries} attempts: {e}. Url: {stock_url}")
                raise


async def get_stock_data_async(stock_id: int, start_date: str, end_date: str) -> list[dict]:
    url = f"https://api.investing.com/api/financialdata/historical/{stock_id}?start-date={start_date}&end-date={end_date}&time-frame=Daily&add-missing-rows=false"

    max_retries = 3
    retry_delays = [1, 3, 5]
    
    for attempt in range(max_retries):
        try:
            async with AsyncSession() as client:
                response = await client.get(
                    url, 
                    timeout=30, 
                    impersonate="chrome120", 
                    headers={"Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7", "Domain-Id": "ru"}
                )

                data = response.json().get('data', [])

                if not isinstance(data, (list, tuple)):
                    raise ValueError(f"Data is not iterable: {data}")

                results = []
                for row in data:
                    date = row['rowDate']
                    close_price = row['last_close']
                    results.append({
                        'date': date,
                        'close_price': close_price
                    })
                
                return results
                
        except Exception as e:
            if attempt < max_retries - 1:
                delay = retry_delays[attempt]
                logger.warning(f"Investing get_stock_data error (attempt {attempt + 1}/{max_retries}): {e}. Retrying in {delay}s...")
                await asyncio.sleep(delay)
            else:
                logger.error(f"Investing get_stock_data failed after {max_retries} attempts: {e}")
                raise


async def get_investing_price_async(stock_url: str, target_date: str) -> tuple[float | None, str | None]:
    stock_id, currency = await get_stock_id_async(stock_url)
    await asyncio.sleep(0.5)
    results = await get_stock_data_async(stock_id, target_date, target_date)
    price = results[0].get('close_price') if results else None
    return price, currency

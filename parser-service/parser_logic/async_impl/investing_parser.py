import json
import re
from curl_cffi.requests import AsyncSession
from bs4 import BeautifulSoup


async def get_stock_id_async(stock_url: str) -> int:
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
        
        return stock_id


async def get_stock_data_async(stock_id: int, start_date: str, end_date: str) -> list[dict]:
    url = f"https://api.investing.com/api/financialdata/historical/{stock_id}?start-date={start_date}&end-date={end_date}&time-frame=Daily&add-missing-rows=false"

    async with AsyncSession() as client:
        response = await client.get(
            url, 
            timeout=30, 
            impersonate="chrome120", 
            headers={"Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7", "Domain-Id": "ru"}
        )

        data = response.json()['data']
        results = []
        for row in data:
            date = row['rowDate']
            close_price = row['last_close']
            results.append({
                'date': date,
                'close_price': close_price
            })
        
        return results


async def get_investing_price_async(stock_url: str, target_date: str) -> float | None:
    stock_id = await get_stock_id_async(stock_url)
    results = await get_stock_data_async(stock_id, target_date, target_date)
    if results:
        return results[0].get('close_price')
    return None


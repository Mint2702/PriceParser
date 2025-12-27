import json
from datetime import datetime, timedelta
from curl_cffi.requests import AsyncSession


async def parse_moex_stock_async(ticker: str, target_date: str) -> list[dict]:
    date_obj = datetime.strptime(target_date, '%Y-%m-%d')
    from_date = (date_obj - timedelta(days=30)).strftime('%Y-%m-%d')
    till_date = (date_obj + timedelta(days=1)).strftime('%Y-%m-%d')
    
    url = f"https://iss.moex.com/iss/history/engines/otc/markets/shares/boardgroups/1258/securities/{ticker}-RM.jsonp?iss.meta=off&iss.json=extended&callback=JSON_CALLBACK&lang=ru&from={from_date}&till={till_date}&start=0&limit=100&sort_column=TRADEDATE&sort_order=desc"

    async with AsyncSession() as client:
        response = await client.get(
            url, 
            timeout=30, 
            impersonate="chrome120", 
            cookies={"bh": "Ek8iTm90KUE7QnJhbmQiO3Y9IjgiLCAiQ2hyb21pdW0iO3Y9IjEzOCIsICJZYUJyb3dzZXIiO3Y9IjI1LjgiLCAiWW93c2VyIjt2PSIyLjUiGgUiYXJtIioCPzA6ByJtYWNPUyJCCCIxNS42LjAiSgQiNjQiUmYiTm90KUE7QnJhbmQiO3Y9IjguMC4wLjAiLCAiQ2hyb21pdW0iO3Y9IjEzOC4wLjcyMDQuOTc3IiwgIllhQnJvd3NlciI7dj0iMjUuOC41Ljk3NyIsICJZb3dzZXIiO3Y9IjIuNSJaAj8wYOvS3MkGaiPcytG2Abvxn6sE"}
        )
        response.raise_for_status()

        data = response.text.split("(")[1].split(")")[0]
        data = json.loads(data)[1]

        results = []
        history = data["history"]
        for row in history:
            short_name = row["SHORTNAME"]
            close_price = row["CLOSE"]
            trade_date = row["TRADEDATE"]
            results.append({
                'short_name': short_name,
                'close_price': close_price,
                'date': trade_date
            })
        
        return results


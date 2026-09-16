import re
from datetime import datetime, timezone
from urllib.parse import urlparse
from curl_cffi import requests
from bs4 import BeautifulSoup

_session = None


def _get_session():
    global _session
    if _session is None:
        _session = requests.Session()
    return _session


def _domain_id(stock_url: str | None) -> str:
    if not stock_url:
        return 'www'
    host = urlparse(stock_url).netloc.lower()
    if host.startswith('ru.'):
        return 'ru'
    return 'www'


def _origin(stock_url: str | None) -> str:
    if not stock_url:
        return 'https://www.investing.com'
    parsed = urlparse(stock_url)
    return f"{parsed.scheme}://{parsed.netloc}"


def _page_headers(stock_url: str) -> dict[str, str]:
    return {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "Domain-Id": _domain_id(stock_url),
    }


def _api_headers(stock_url: str | None) -> dict[str, str]:
    return {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "Domain-Id": _domain_id(stock_url),
        "Origin": _origin(stock_url),
        "Referer": stock_url or "https://www.investing.com/",
    }


def _to_float(value) -> float | None:
    if value is None or value == '':
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if ',' in text and '.' in text:
        if text.rfind(',') > text.rfind('.'):
            text = text.replace('.', '').replace(',', '.')
        else:
            text = text.replace(',', '')
    elif ',' in text:
        text = text.replace(',', '.')
    try:
        return float(text)
    except ValueError:
        return None


def _parse_row_date(row: dict) -> str | None:
    raw_ts = row.get('rowDateRaw')
    if raw_ts is not None:
        try:
            ts = int(raw_ts)
            if ts > 10**12:
                ts //= 1000
            return datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%d')
        except (TypeError, ValueError, OSError, OverflowError):
            pass
    raw = row.get('rowDate')
    if not raw:
        return None
    for fmt in ('%b %d, %Y', '%d/%m/%Y', '%d.%m.%Y', '%Y-%m-%d', '%m/%d/%Y'):
        try:
            return datetime.strptime(str(raw), fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    return None


def get_stock_id(stock_url: str) -> int:
    client = _get_session()
    response = client.get(
        stock_url,
        timeout=30,
        impersonate="chrome131",
        headers=_page_headers(stock_url),
    )
    data = response.text

    soup = BeautifulSoup(data, "html.parser")
    script_tag = soup.find("script", id="__NEXT_DATA__")
    if script_tag is None:
        raise ValueError(f"__NEXT_DATA__ not found (status={response.status_code}, cloudflare challenge)")
    script_data = script_tag.text
    
    match = re.search(r'"identifiers"\s*:\s*\{[^}]*"instrument_id"\s*:\s*"?(\d+)"?', script_data)
    if match:
        stock_id = int(match.group(1))
    else:
        raise ValueError("instrument_id not found in identifiers object")
    
    return stock_id


def get_stock_data(stock_id: int, start_date: str, end_date: str, stock_url: str | None = None) -> list[dict]:
    url = f"https://api.investing.com/api/financialdata/historical/{stock_id}?start-date={start_date}&end-date={end_date}&time-frame=Daily&add-missing-rows=false"

    client = _get_session()
    response = client.get(
        url,
        timeout=30,
        impersonate="chrome131",
        headers=_api_headers(stock_url),
    )

    try:
        payload = response.json()
    except Exception:
        return []

    data = payload.get('data') if isinstance(payload, dict) else None
    if not isinstance(data, (list, tuple)):
        return []

    results = []
    for row in data:
        if not isinstance(row, dict):
            continue
        row_date = _parse_row_date(row)
        close_price = _to_float(row.get('last_close') or row.get('lastClose') or row.get('close'))
        if row_date is None or close_price is None:
            continue
        results.append({
            'date': row_date,
            'close_price': close_price
        })
    
    return results

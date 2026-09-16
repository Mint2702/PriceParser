import asyncio
import re
import logging
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse
from curl_cffi.requests import AsyncSession
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

HISTORY_LOOKBACK_DAYS = 10
RETRY_DELAYS = [2, 4, 8]
RETRY_DELAYS_429 = [8, 20, 60]
RETRYABLE_STATUS = {403, 429, 500, 502, 503, 504}
INVESTING_CONCURRENCY = 2

STOCK_ID_CACHE: dict[str, int] = {}

_semaphore: asyncio.Semaphore | None = None


class RetryableInvestingError(Exception):
    pass


def _get_semaphore() -> asyncio.Semaphore:
    global _semaphore
    if _semaphore is None:
        _semaphore = asyncio.Semaphore(INVESTING_CONCURRENCY)
    return _semaphore


def _is_retryable_exception(exc: Exception) -> bool:
    if isinstance(exc, RetryableInvestingError):
        return True
    name = type(exc).__name__.lower()
    return any(token in name for token in ('timeout', 'connection', 'connecterror', 'network'))


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


def _pick_close_price(results: list[dict], target_date: str) -> float | None:
    candidates = [
        (row['date'], row['close_price'])
        for row in results
        if row.get('date') and row.get('close_price') is not None and row['date'] <= target_date
    ]
    if not candidates:
        return None
    exact = [price for date, price in candidates if date == target_date]
    if exact:
        return exact[-1]
    candidates.sort(key=lambda item: item[0])
    used_date, price = candidates[-1]
    logger.info(f"Investing has no candle for {target_date}, using {used_date}")
    return price


async def _retry(action, *, what: str, extra: str = ""):
    attempt = 0
    while True:
        try:
            return await action()
        except Exception as e:
            retryable = _is_retryable_exception(e)
            delays = RETRY_DELAYS_429 if '429' in str(e) else RETRY_DELAYS
            max_retries = len(delays) + 1
            suffix = f" {extra}" if extra else ""
            if retryable and attempt < max_retries - 1:
                delay = delays[attempt]
                logger.warning(
                    f"Investing {what} error (attempt {attempt + 1}/{max_retries}): {e}. "
                    f"Retrying in {delay}s...{suffix}"
                )
                await asyncio.sleep(delay)
                attempt += 1
                continue
            logger.error(
                f"Investing {what} failed after {attempt + 1} attempts: {e}.{suffix}"
            )
            raise


async def _load_stock_id(session: AsyncSession, stock_url: str) -> int:
    cached = STOCK_ID_CACHE.get(stock_url)
    response = await session.get(
        stock_url,
        timeout=30,
        impersonate="chrome131",
        headers=_page_headers(stock_url),
    )
    if response.status_code in RETRYABLE_STATUS:
        raise RetryableInvestingError(f"HTTP {response.status_code}")

    soup = BeautifulSoup(response.text, "html.parser")
    script_tag = soup.find("script", id="__NEXT_DATA__")
    if script_tag is None:
        snippet = response.text[:200].replace('\n', ' ')
        raise RetryableInvestingError(
            f"__NEXT_DATA__ not found (status={response.status_code}): {snippet!r}"
        )
    if cached is not None:
        return cached
    match = re.search(
        r'"identifiers"\s*:\s*\{[^}]*"instrument_id"\s*:\s*"?(\d+)"?',
        script_tag.text,
    )
    if not match:
        raise ValueError("instrument_id not found in identifiers object")
    stock_id = int(match.group(1))
    STOCK_ID_CACHE[stock_url] = stock_id
    return stock_id


async def _load_stock_data(
    session: AsyncSession,
    stock_id: int,
    start_date: str,
    end_date: str,
    stock_url: str | None = None,
) -> list[dict]:
    url = (
        f"https://api.investing.com/api/financialdata/historical/{stock_id}"
        f"?start-date={start_date}&end-date={end_date}&time-frame=Daily&add-missing-rows=false"
    )
    response = await session.get(
        url,
        timeout=30,
        impersonate="chrome131",
        headers=_api_headers(stock_url),
    )
    if response.status_code in RETRYABLE_STATUS:
        raise RetryableInvestingError(f"HTTP {response.status_code}: {response.text[:300]!r}")

    try:
        payload = response.json()
    except Exception as e:
        logger.warning(
            f"Investing historical non-JSON (status={response.status_code}, url={url}): "
            f"{response.text[:300]!r}"
        )
        raise RetryableInvestingError(
            f"Non-JSON historical response status={response.status_code}"
        ) from e

    data = payload.get('data') if isinstance(payload, dict) else None
    if not isinstance(data, (list, tuple)):
        logger.warning(
            f"Investing historical empty (status={response.status_code}, "
            f"data={data!r}, url={url})"
        )
        return []

    results = []
    for row in data:
        if not isinstance(row, dict):
            continue
        row_date = _parse_row_date(row)
        close_price = _to_float(
            row.get('last_close') or row.get('lastClose') or row.get('close')
        )
        if row_date is None or close_price is None:
            continue
        results.append({
            'date': row_date,
            'close_price': close_price,
        })
    return results


async def get_stock_id_async(stock_url: str) -> int:
    async def _fetch() -> int:
        async with AsyncSession() as session:
            return await _load_stock_id(session, stock_url)

    return await _retry(_fetch, what="get_stock_id", extra=f"Url: {stock_url}")


async def get_stock_data_async(
    stock_id: int,
    start_date: str,
    end_date: str,
    stock_url: str | None = None,
) -> list[dict]:
    async def _fetch() -> list[dict]:
        async with AsyncSession() as session:
            return await _load_stock_data(session, stock_id, start_date, end_date, stock_url)

    return await _retry(_fetch, what="get_stock_data")


async def get_investing_price_async(stock_url: str, target_date: str) -> float | None:
    start_date = (
        datetime.strptime(target_date, '%Y-%m-%d') - timedelta(days=HISTORY_LOOKBACK_DAYS)
    ).strftime('%Y-%m-%d')

    async def _fetch() -> float | None:
        async with AsyncSession() as session:
            stock_id = await _load_stock_id(session, stock_url)
            await asyncio.sleep(0.3)
            results = await _load_stock_data(
                session, stock_id, start_date, target_date, stock_url
            )
            return _pick_close_price(results, target_date)

    async with _get_semaphore():
        return await _retry(_fetch, what="get_price", extra=f"Url: {stock_url}")

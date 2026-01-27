#!/usr/bin/env python3
"""
Debug script to test parsing a single stock.
Useful for testing and debugging the parsers.
"""
import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "parser_service"))

import argparse
from datetime import datetime
from sync import parse_moex_stock, get_stock_id, get_stock_data

logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


def parse_date(date_str: str) -> str:
    """Parse date from DD.MM.YYYY to YYYY-MM-DD format."""
    try:
        date_obj = datetime.strptime(date_str, '%d.%m.%Y')
        return date_obj.strftime('%Y-%m-%d')
    except ValueError:
        try:
            datetime.strptime(date_str, '%Y-%m-%d')
            return date_str
        except ValueError:
            raise ValueError(f"Invalid date format: {date_str}. Use DD.MM.YYYY or YYYY-MM-DD")


def test_moex(ticker: str, date: str):
    """Test MOEX parser for a single stock."""
    logger.info(f"\n{'='*60}")
    logger.info(f"Testing MOEX Parser")
    logger.info(f"{'='*60}")
    logger.info(f"Ticker: {ticker}")
    logger.info(f"Date: {date}")
    logger.info(f"URL: https://iss.moex.com/...")
    logger.info(f"-"*60)
    
    try:
        results = parse_moex_stock(ticker, date)
        
        if not results:
            logger.warning("No results returned")
            return None
        
        logger.info(f"✓ Found {len(results)} price entries")
        logger.info(f"\nAll available dates:")
        for entry in results[:5]:
            logger.info(f"  - {entry['date']}: {entry['close_price']} RUB ({entry['short_name']})")
            logger.info(f"    Trades: {entry.get('num_trades', 'N/A')}, Volume: {entry.get('volume', 'N/A')}")
        
        if len(results) > 5:
            logger.info(f"  ... and {len(results) - 5} more")
        
        for entry in results:
            if entry['date'] == date:
                logger.info(f"\n✓ Price for {date}: {entry['close_price']} RUB")
                logger.info(f"  Trades: {entry.get('num_trades', 'N/A')}")
                logger.info(f"  Volume: {entry.get('volume', 'N/A')}")
                return entry['close_price']
        
        logger.warning(f"No price found for specific date: {date}")
        return None
        
    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return None


def test_investing(url: str, date: str):
    """Test Investing.com parser for a single stock."""
    logger.info(f"\n{'='*60}")
    logger.info(f"Testing Investing.com Parser")
    logger.info(f"{'='*60}")
    logger.info(f"URL: {url}")
    logger.info(f"Date: {date}")
    logger.info(f"-"*60)
    
    try:
        logger.info("Step 1: Getting stock ID...")
        stock_id = get_stock_id(url)
        logger.info(f"✓ Stock ID: {stock_id}")
        
        logger.info(f"\nStep 2: Fetching price data...")
        results = get_stock_data(stock_id, date, date)
        
        if not results:
            logger.warning("No results returned")
            return None
        
        logger.info(f"✓ Found {len(results)} price entries")
        
        for entry in results:
            logger.info(f"\nPrice data:")
            logger.info(f"  Date: {entry['date']}")
            logger.info(f"  Close: ${entry['close_price']}")
            return entry['close_price']
        
        logger.warning(f"No price found for date: {date}")
        return None
        
    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return None


def main():
    parser = argparse.ArgumentParser(
        description='Test parsing a single stock (debug tool)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Test MOEX only
  %(prog)s --ticker TWOU --date 31.10.2025
  
  # Test Investing.com only
  %(prog)s --investing-url "https://ru.investing.com/equities/3m-co-historical-data" --date 31.10.2025
  
  # Test both
  %(prog)s --ticker MMM --investing-url "https://ru.investing.com/equities/3m-co-historical-data" --date 31.10.2025
        """
    )
    
    parser.add_argument(
        '-t', '--ticker',
        type=str,
        help='Stock ticker for MOEX (e.g., TWOU, MMM)'
    )
    
    parser.add_argument(
        '-u', '--investing-url',
        type=str,
        help='Investing.com historical data URL'
    )
    
    parser.add_argument(
        '-d', '--date',
        type=str,
        required=True,
        help='Date in DD.MM.YYYY or YYYY-MM-DD format'
    )
    
    args = parser.parse_args()
    
    if not args.ticker and not args.investing_url:
        logger.error("Provide at least --ticker or --investing-url")
        parser.print_help()
        sys.exit(1)
    
    try:
        date = parse_date(args.date)
    except ValueError as e:
        logger.error(f"Error: {e}")
        sys.exit(1)
    
    logger.info(f"\n{'#'*60}")
    logger.info(f"Single Stock Debug Tool")
    logger.info(f"{'#'*60}")
    
    moex_price = None
    investing_price = None
    
    if args.ticker:
        moex_price = test_moex(args.ticker, date)
    
    if args.investing_url:
        if not args.ticker or moex_price is not None:
            investing_price = test_investing(args.investing_url, date)
        else:
            logger.info(f"\n{'='*60}")
            logger.info("Skipping Investing.com (no MOEX price found)")
            logger.info(f"{'='*60}")
    
    logger.info(f"\n{'#'*60}")
    logger.info("Summary")
    logger.info(f"{'#'*60}")
    
    if args.ticker:
        status = "✓" if moex_price is not None else "✗"
        price = f"{moex_price} RUB" if moex_price is not None else "Not found"
        logger.info(f"MOEX ({args.ticker}): {status} {price}")
    
    if args.investing_url:
        status = "✓" if investing_price is not None else "✗"
        price = f"${investing_price}" if investing_price is not None else "Not found"
        logger.info(f"Investing.com: {status} {price}")
    
    logger.info(f"{'#'*60}\n")
    
    sys.exit(0 if (moex_price or investing_price) else 1)


if __name__ == '__main__':
    main()

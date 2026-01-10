#!/usr/bin/env python3
"""
Debug script to test parsing a single stock using async methods.
Useful for testing and debugging the async parsers.
"""
import sys

import asyncio
import argparse
from datetime import datetime
from parser_service.async_impl import parse_moex_stock_async, get_stock_id_async, get_stock_data_async, get_investing_price_async


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


async def test_moex(ticker: str, date: str):
    """Test async MOEX parser for a single stock."""
    print(f"\n{'='*60}")
    print(f"Testing MOEX Parser (ASYNC)")
    print(f"{'='*60}")
    print(f"Ticker: {ticker}")
    print(f"Date: {date}")
    print(f"URL: https://iss.moex.com/...")
    print(f"-"*60)
    
    try:
        results = await parse_moex_stock_async(ticker, date)
        
        if not results:
            print("❌ No results returned")
            return None
        
        print(f"✓ Found {len(results)} price entries")
        print(f"\nAll available dates:")
        for entry in results[:5]:
            print(f"  - {entry['date']}: {entry['close_price']} RUB ({entry['short_name']})")
            print(f"    Trades: {entry.get('num_trades', 'N/A')}, Volume: {entry.get('volume', 'N/A')}")
        
        if len(results) > 5:
            print(f"  ... and {len(results) - 5} more")
        
        for entry in results:
            if entry['date'] == date:
                print(f"\n✓ Price for {date}: {entry['close_price']} RUB")
                print(f"  Trades: {entry.get('num_trades', 'N/A')}")
                print(f"  Volume: {entry.get('volume', 'N/A')}")
                return entry['close_price']
        
        print(f"\n❌ No price found for specific date: {date}")
        return None
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return None


async def test_investing(url: str, date: str):
    """Test async Investing.com parser for a single stock."""
    print(f"\n{'='*60}")
    print(f"Testing Investing.com Parser (ASYNC)")
    print(f"{'='*60}")
    print(f"URL: {url}")
    print(f"Date: {date}")
    print(f"-"*60)
    
    try:
        print("Step 1: Getting stock ID...")
        stock_id = await get_stock_id_async(url)
        print(f"✓ Stock ID: {stock_id}")
        
        print(f"\nStep 2: Fetching price data...")
        results = await get_stock_data_async(stock_id, date, date)
        
        if not results:
            print("❌ No results returned")
            return None
        
        print(f"✓ Found {len(results)} price entries")
        
        for entry in results:
            print(f"\nPrice data:")
            print(f"  Date: {entry['date']}")
            print(f"  Close: ${entry['close_price']}")
            return entry['close_price']
        
        print(f"\n❌ No price found for date: {date}")
        return None
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return None


async def main_async(args):
    print(f"\n{'#'*60}")
    print(f"Single Stock Debug Tool (ASYNC)")
    print(f"{'#'*60}")
    
    moex_price = None
    investing_price = None
    
    if args.ticker:
        moex_price = await test_moex(args.ticker, args.date)
    
    if args.investing_url:
        if not args.ticker or moex_price is not None:
            investing_price = await test_investing(args.investing_url, args.date)
        else:
            print(f"\n{'='*60}")
            print("Skipping Investing.com (no MOEX price found)")
            print(f"{'='*60}")
    
    print(f"\n{'#'*60}")
    print("Summary")
    print(f"{'#'*60}")
    
    if args.ticker:
        status = "✓" if moex_price is not None else "✗"
        price = f"{moex_price} RUB" if moex_price is not None else "Not found"
        print(f"MOEX ({args.ticker}): {status} {price}")
    
    if args.investing_url:
        status = "✓" if investing_price is not None else "✗"
        price = f"${investing_price}" if investing_price is not None else "Not found"
        print(f"Investing.com: {status} {price}")
    
    print(f"{'#'*60}\n")
    
    return 0 if (moex_price or investing_price) else 1


def main():
    parser = argparse.ArgumentParser(
        description='Test parsing a single stock using async methods (debug tool)',
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
        print("Error: Provide at least --ticker or --investing-url", file=sys.stderr)
        parser.print_help()
        sys.exit(1)
    
    try:
        args.date = parse_date(args.date)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    
    exit_code = asyncio.run(main_async(args))
    sys.exit(exit_code)


if __name__ == '__main__':
    main()

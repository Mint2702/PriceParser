#!/bin/bash

show_usage() {
    cat << EOF
Usage: $(basename "$0") [OPTIONS]

Debug tool to test stock price parsing with sync or async implementation.

OPTIONS:
    -s              Use sync implementation (default)
    -a              Use async implementation
    -t TICKER       Stock ticker for MOEX (e.g., ROCU, TWOU, MMM)
    -u URL          Investing.com historical data URL
    -d DATE         Date in DD.MM.YYYY or YYYY-MM-DD format (required)
    -h              Show this help message

EXAMPLES:
    # Test MOEX with async
    $(basename "$0") -a -t ROCU -d 2025-12-29

    # Test MOEX with sync
    $(basename "$0") -s -t TWOU -d 31.10.2025

    # Test both MOEX and Investing.com
    $(basename "$0") -a -t MMM -u "https://ru.investing.com/equities/3m-co-historical-data" -d 2025-12-29

    # Test Investing.com only
    $(basename "$0") -a -u "https://ru.investing.com/equities/3m-co-historical-data" -d 2025-12-29

EOF
}

MODE="sync"
TICKER=""
INVESTING_URL=""
DATE=""

while getopts "sat:u:d:h" opt; do
    case $opt in
        s)
            MODE="sync"
            ;;
        a)
            MODE="async"
            ;;
        t)
            TICKER="$OPTARG"
            ;;
        u)
            INVESTING_URL="$OPTARG"
            ;;
        d)
            DATE="$OPTARG"
            ;;
        h)
            show_usage
            exit 0
            ;;
        \?)
            echo "Invalid option: -$OPTARG" >&2
            show_usage
            exit 1
            ;;
    esac
done

if [ -z "$DATE" ]; then
    echo "Error: Date (-d) is required" >&2
    show_usage
    exit 1
fi

if [ -z "$TICKER" ] && [ -z "$INVESTING_URL" ]; then
    echo "Error: Provide at least -t (ticker) or -u (investing URL)" >&2
    show_usage
    exit 1
fi

CMD_ARGS="-d $DATE"

if [ -n "$TICKER" ]; then
    CMD_ARGS="$CMD_ARGS -t $TICKER"
fi

if [ -n "$INVESTING_URL" ]; then
    CMD_ARGS="$CMD_ARGS -u \"$INVESTING_URL\""
fi

if [ "$MODE" = "async" ]; then
    echo "Running async parser..."
    eval "python -m adhoc.test_single_stock_async $CMD_ARGS"
else
    echo "Running sync parser..."
    eval "python -m adhoc.test_single_stock $CMD_ARGS"
fi

import asyncio
from datetime import datetime, timedelta
import redis.asyncio as redis
import requests
from cryptofeed.exchanges import BinanceFutures
import time

redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

# KLINE DATA FOR FUTURES
def get_klines_futures(symbol: str, interval: str, limit: int = 100, start_time: int = None, end_time: int = None):
    url = "https://fapi.binance.com/fapi/v1/klines"
    params = {
        "symbol": symbol.upper(),
        "interval": interval,
        "limit": limit
    }
    if start_time:
        params["startTime"] = start_time
    if end_time:
        params["endTime"] = end_time

    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

# KLINE DATA FOR SPOT
def get_klines_spot(symbol: str, interval: str, limit: int = 100, start_time: int = None, end_time: int = None):
    url = "https://api.binance.com/api/v3/klines"
    params = {
        "symbol": symbol.upper(),
        "interval": interval,
        "limit": limit
    }
    if start_time:
        params["startTime"] = start_time
    if end_time:
        params["endTime"] = end_time

    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

def _get_klines_for_period(symbol: str, interval: str, period_timedelta: timedelta, kline_fetch_function):
    """
    Generic function to fetch klines for a given period (e.g., 1 day),
    handling pagination if the number of candles exceeds the API limit.
    """
    BINANCE_LIMIT = 1000  # Binance API limit per request

    end_dt = datetime.utcnow()
    start_dt = end_dt - period_timedelta

    # Convert to milliseconds for Binance API
    end_ms = int(end_dt.timestamp() * 1000)
    start_ms = int(start_dt.timestamp() * 1000)

    all_klines = []
    fetch_start_time = start_ms

    while True:
        # Fetch klines in chunks from the start time until the end time
        klines = kline_fetch_function(
            symbol=symbol,
            interval=interval,
            limit=BINANCE_LIMIT,
            start_time=fetch_start_time,
            end_time=end_ms
        )

        if not klines:
            # No more data in the range
            break

        all_klines.extend(klines)

        # The next fetch should start right after the last candle we received
        fetch_start_time = int(klines[-1][0]) + 1

        # If we received fewer klines than the limit, it means we got all data up to the end_time
        if len(klines) < BINANCE_LIMIT:
            break

    # Deduplicate results just in case there are overlaps
    unique_klines_dict = {k[0]: k for k in all_klines}
    sorted_klines = sorted(unique_klines_dict.values(), key=lambda k: k[0])

    return sorted_klines

def get_klines_futures_for_day(symbol: str, interval: str):
    """Fetches the last 24 hours of kline data for a futures symbol."""
    return _get_klines_for_period(symbol, interval, timedelta(days=1), get_klines_futures)

def get_klines_spot_for_day(symbol: str, interval: str):
    """Fetches the last 24 hours of kline data for a spot symbol."""
    return _get_klines_for_period(symbol, interval, timedelta(days=1), get_klines_spot)

async def main():
# Get FUTURES Klines data
    print("\n=== klines data for FUTURE ===")
    klines_futures= get_klines_futures("BTCUSDT", "1h", limit=5)
    for kline in klines_futures:
        print(kline)
        
    # Get SPOT Klines data
    print("\n=== klines data for SPOT ===")
    klines_spot = get_klines_spot("BTCUSDT", "1h", limit=5)
    for kline in klines_spot:
        print(kline)
        
if __name__ == "__main__":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
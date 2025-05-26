import asyncio
from datetime import datetime
import redis.asyncio as redis
import requests
from cryptofeed.exchanges import BinanceFutures

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
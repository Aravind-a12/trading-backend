import logging
import asyncio
from datetime import datetime
import redis.asyncio as redis
import requests
from cryptofeed.exchanges import BinanceFutures

redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

# async def check_redis_connection():
#     try:
#         await redis_client.ping()
#         print("✅ Redis Connected Successfully!")
#     except Exception as e:
#         print(f"❌ Redis Connection Error: {e}")
#         exit()

# AGGREGATE TRADES FOR FUTURES
def get_aggregate_trades_futures(symbol: str, limit: int = 1):
    url = "https://fapi.binance.com/fapi/v1/aggTrades"
    params = {
        "symbol": symbol.upper(),
        "limit": limit
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

# AGGREGATE TRADES FOR SPOT
def get_aggregate_trades_spot(symbol: str, limit: int = 1):
    url = "https://api.binance.com/api/v3/aggTrades"
    params = {
        "symbol": symbol.upper(),
        "limit": limit
    }

    response = requests.get(url, params=params)
    response.raise_for_status()  # Raise an error if request failed
    return response.json()
  
async def main():
  
    # Get FUTURES aggregate trades
    print("\n=== Aggregate Trades for FUTURES ===")
    trades_futures = get_aggregate_trades_futures("BTCUSDT", limit=1)
    for trade in trades_futures:
        print(trade)

    # Get SPOT aggregate trades
    print("\n=== Aggregate Trades for SPOT ===")
    trades_spot = get_aggregate_trades_spot("BTCUSDT", limit=1)
    for trade in trades_spot:
        print(trade)
    
if __name__ == "__main__":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    # asyncio.run(check_redis_connection())
    asyncio.run(main())


        
        
import logging
import asyncio
from datetime import datetime
import redis.asyncio as redis
import json
import decimal
import requests
from cryptofeed.exchanges import BinanceFutures

redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

# EXCHANGE INFORMATION FOR FUTURES
def store_futures_exchange_info():
    try:
        url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        all_symbols = data.get("symbols", [])
        redis_client.set("exchange_info:all", json.dumps(all_symbols))

        for symbol in all_symbols:
            symbol_name = symbol.get("symbol")
            redis_client.set(f"exchange_info:{symbol_name}", json.dumps(symbol))

        print(f"✅ Stored {len(all_symbols)} exchange info entries in Redis")
        for symbol in all_symbols:
            print(f"Symbol: {symbol.get('symbol')}")
            print(f"  Base Asset     : {symbol.get('baseAsset')}")
            print(f"  Quote Asset    : {symbol.get('quoteAsset')}")
            print(f"  Status         : {symbol.get('status')}")
            print(f"  Margin Asset   : {symbol.get('marginAsset')}")
            print(f"  Contract Type  : {symbol.get('contractType')}")
            print(f"  Price Precision: {symbol.get('pricePrecision')}")
            print(f"  Quantity Prec. : {symbol.get('quantityPrecision')}")

        return all_symbols  # ← ADD THIS LINE
    except Exception as e:
        print(f"❌ Failed to fetch/store exchange info: {e}")
        return {"error": str(e)}  # optional: to provide error response to API

# EXCHANGE INFORMATION FOR SPOT
def store_spot_exchange_info():
    try:
        url = "https://api.binance.com/api/v3/exchangeInfo"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        all_symbols = data.get("symbols", [])
        redis_client.set("exchange_info_spot:all", json.dumps(all_symbols))

        for symbol in all_symbols:
            symbol_name = symbol.get("symbol")
            redis_client.set(f"exchange_info_spot:{symbol_name}", json.dumps(symbol))

        print(f"✅ Stored {len(all_symbols)} spot exchange info entries in Redis")
        for symbol in all_symbols:
            print(f"Symbol: {symbol.get('symbol')}")
            print(f"  Base Asset     : {symbol.get('baseAsset')}")
            print(f"  Quote Asset    : {symbol.get('quoteAsset')}")
            print(f"  Status         : {symbol.get('status')}")
            print(f"  Price Precision: {symbol.get('pricePrecision')}")
            print(f"  Quantity Prec. : {symbol.get('quantityPrecision')}")
        return all_symbols  
    except Exception as e:
        print(f"❌ Failed to fetch/store spot exchange info: {e}")
        
async def main():
    print("\n📦 Storing Binance Futures Exchange Info...")
    store_futures_exchange_info()

    print("\n📦 Storing Binance Spot Exchange Info...")
    store_spot_exchange_info()
    
    
if __name__ == "__main__":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())

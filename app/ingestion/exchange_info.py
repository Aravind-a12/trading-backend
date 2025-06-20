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
def store_futures_exchange_info(symbol: str = None):
    try:
        url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        all_symbols = data.get("symbols", [])

        redis_client.set("exchange_info:all", json.dumps(all_symbols))

        for item in all_symbols:
            sym_name = item.get("symbol")
            redis_client.set(f"exchange_info:{sym_name}", json.dumps(item))

        print(f"✅ Stored {len(all_symbols)} exchange info entries in Redis")

        # Return specific symbol info if requested
        if symbol:
            filtered = next((item for item in all_symbols if item.get("symbol") == symbol), None)
            return filtered if filtered else {"error": f"Symbol '{symbol}' not found."}

        # Print a sample for debugging
        for item in all_symbols:
            print(f"Symbol: {item.get('symbol')}")
            print(f"  Base Asset     : {item.get('baseAsset')}")
            print(f"  Quote Asset    : {item.get('quoteAsset')}")
            print(f"  Status         : {item.get('status')}")
            print(f"  Margin Asset   : {item.get('marginAsset')}")
            print(f"  Contract Type  : {item.get('contractType')}")
            print(f"  Price Precision: {item.get('pricePrecision')}")
            print(f"  Quantity Prec. : {item.get('quantityPrecision')}")

        return all_symbols
    except Exception as e:
        print(f"❌ Failed to fetch/store exchange info: {e}")
        return {"error": str(e)}

# EXCHANGE INFORMATION FOR SPOT
def store_spot_exchange_info(symbol: str = None):
    try:
        url = "https://api.binance.com/api/v3/exchangeInfo"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        all_symbols = data.get("symbols", [])

        redis_client.set("exchange_info_spot:all", json.dumps(all_symbols))

        for item in all_symbols:
            sym_name = item.get("symbol")
            redis_client.set(f"exchange_info_spot:{sym_name}", json.dumps(item))

        print(f"✅ Stored {len(all_symbols)} spot exchange info entries in Redis")

        if symbol:
            filtered = next((item for item in all_symbols if item.get("symbol") == symbol), None)
            return filtered if filtered else {"error": f"Symbol '{symbol}' not found."}

        for item in all_symbols:
            print(f"Symbol: {item.get('symbol')}")
            print(f"  Base Asset     : {item.get('baseAsset')}")
            print(f"  Quote Asset    : {item.get('quoteAsset')}")
            print(f"  Status         : {item.get('status')}")
            print(f"  Price Precision: {item.get('pricePrecision')}")
            print(f"  Quantity Prec. : {item.get('quantityPrecision')}")

        return all_symbols
    except Exception as e:
        print(f"❌ Failed to fetch/store spot exchange info: {e}")
        return {"error": str(e)}

# Run manually
async def main():
    print("\n📦 Storing Binance Futures Exchange Info...")
    store_futures_exchange_info()

    print("\n📦 Storing Binance Spot Exchange Info...")
    store_spot_exchange_info()

if __name__ == "__main__":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
import logging
import asyncio
from datetime import datetime
import redis.asyncio as redis
import json
import decimal
import requests
import aiohttp
from cryptofeed import FeedHandler
from cryptofeed.exchanges import BinanceFutures
from cryptofeed.defines import TRADES, OPEN_INTEREST, L2_BOOK,FUNDING,TICKER

logging.basicConfig(level=logging.INFO)
logging.getLogger('cryptofeed').setLevel(logging.DEBUG)

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        return super().default(obj)

redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

async def check_redis_connection():
    try:
        await redis_client.ping()
        print("✅ Redis Connected Successfully!")
    except Exception as e:
        print(f"❌ Redis Connection Error: {e}")
        exit()

# Candle data structure
candle_data = {}

# Trade callback
async def trade_callback(trade, receipt_timestamp):
    global candle_data
    timestamp = int(trade.timestamp)
    price = float(trade.price)
    volume = float(trade.amount)
    side = trade.side

    print(f"📌 Trade - Price: {price}, Volume: {volume}, Side: {side}, Timestamp: {timestamp}")

    trade_data = {
        "timestamp": datetime.utcfromtimestamp(timestamp).isoformat(),
        "price": price,
        "volume": volume,
        "side": side
    }

    try:
        json_data = json.dumps(trade_data)
        redis_client.zadd("trades", {json_data: timestamp})
        await redis_client.publish("trade",json_data)  # 🔔 publish to pub/sub channel
        print("✅ Stored and Published trade in Redis")
        print("good")

    except Exception as e:
        print(f"❌ Redis Insert Error (Trade): {e}")

    # Handle candle data
    if timestamp not in candle_data:
        if candle_data:
            last_timestamp = next(iter(candle_data))
            last_candle = candle_data[last_timestamp]
            last_candle["timestamp"] = last_candle["timestamp"].isoformat()
            try:
                redis_client.zadd("candles", {json.dumps(last_candle): last_timestamp})
                print("✅ Stored candle in Redis:", last_candle)
            except Exception as e:
                print(f"❌ Redis Insert Error (Candle): {e}")
        candle_data = {
            timestamp: {
                "timestamp": datetime.utcfromtimestamp(timestamp),
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": volume
            }
        }
    else:
        candle = candle_data[timestamp]
        candle["high"] = max(candle["high"], price)
        candle["low"] = min(candle["low"], price)
        candle["close"] = price
        candle["volume"] += volume
        
# Open interest callback
async def open_interest_callback(data, receipt_timestamp):
    open_interest_data = {
        "timestamp": datetime.utcfromtimestamp(data.timestamp).isoformat(),
        "open_interest": float(data.open_interest)
    }

    print(f"📌 Open Interest: {open_interest_data}")

    try:
        redis_client.zadd("open_interest", {json.dumps(open_interest_data): data.timestamp})
        print("✅ Stored open interest in Redis") 
    except Exception as e:
        print(f"❌ Redis Insert Error (Open Interest): {e}")

async def order_book_callback(book, receipt_timestamp):
    try:
        bids = list(book.book.bids)[:10]
        asks = list(book.book.asks)[:10]

        data = {
            "timestamp": datetime.utcnow().isoformat(),
            "bids": [(float(price), float(book.book.bids[price])) for price in bids],
            "asks": [(float(price), float(book.book.asks[price])) for price in asks]
        }

        redis_client.zadd("order_book_snapshots", {json.dumps(data, cls=DecimalEncoder): receipt_timestamp})
        print("✅ Stored order book in Redis")
    except Exception as e:
        print(f"❌ Redis Insert Error (Order Book): {e}")

# Funding rate callback
async def funding_rate_callback(data, receipt_timestamp):
    funding_data = {
        "timestamp": datetime.utcfromtimestamp(data.timestamp).isoformat(),
        "rate": float(data.rate),
        "interval": getattr(data, 'interval', None)
    }

    print(f"📌 Funding Rate: {funding_data}")

    try:
        redis_client.zadd("funding_rate", {json.dumps(funding_data): data.timestamp})
        print("✅ Stored funding rate in Redis")
    except Exception as e:
        print(f"❌ Redis Insert Error (Funding Rate): {e}")

# Ticker callback
async def ticker_callback(data, receipt_timestamp):
    ticker_data = {
        "timestamp": datetime.utcfromtimestamp(data.timestamp).isoformat(),
        "bid": float(data.bid),
        "ask": float(data.ask),
        "last": getattr(data, 'last', None)
    }

    print(f"📌 Ticker: {ticker_data}")

    try:
        redis_client.zadd("ticker", {json.dumps(ticker_data): data.timestamp})
        print("✅ Stored ticker in Redis")
    except Exception as e:
        print(f"❌ Redis Insert Error (Ticker): {e}")
    
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

        # print(f"✅ Stored {len(all_symbols)} exchange info entries in Redis")
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
    print("\n📦 Storing Binance Futures Exchange Info...")
    store_futures_exchange_info()

    print("\n📦 Storing Binance Spot Exchange Info...")
    store_spot_exchange_info()
    
    f = FeedHandler()
    f.add_feed(BinanceFutures(
        symbols=['BTC-USDT-PERP'],
        channels=[TRADES, OPEN_INTEREST, L2_BOOK,FUNDING,TICKER],
        callbacks={
            TRADES: trade_callback,
            OPEN_INTEREST: open_interest_callback,
            L2_BOOK: order_book_callback,
            FUNDING:funding_rate_callback,
            TICKER:ticker_callback,
        }
    ))
    
    print("📡 Binance Futures Feed started... waiting for data")
    f.run()

if __name__ == "__main__":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(check_redis_connection())
    
    # Get FUTURES aggregate trades
    print("\n=== Aggregate Trades for FUTURES ===")
    trades_futures = get_aggregate_trades_futures("BTCUSDT", limit=1)
    for trade in trades_futures:
        print(trade)

    # # Get SPOT aggregate trades
    print("\n=== Aggregate Trades for SPOT ===")
    trades_spot = get_aggregate_trades_spot("BTCUSDT", limit=1)
    for trade in trades_spot:
        print(trade)
    
    # # Get FUTURES Klines data
    print("\n=== klines data for FUTURE ===")
    klines_futures= get_klines_futures("BTCUSDT", "1h", limit=5)
    for kline in klines_futures:
        print(kline)
        
    # # Get SPOT Klines data
    print("\n=== klines data for SPOT ===")
    klines_spot = get_klines_spot("BTCUSDT", "1h", limit=5)
    for kline in klines_spot:
        print(kline)
    asyncio.run(main())

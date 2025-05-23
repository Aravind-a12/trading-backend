import logging
import asyncio
from datetime import datetime
import redis.asyncio as redis
import json
import decimal
from cryptofeed import FeedHandler
from cryptofeed.exchanges import BinanceFutures
from cryptofeed.defines import TRADES, OPEN_INTEREST, L2_BOOK, FUNDING, TICKER

# Configure logging
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

async def publish_and_store(channel: str, redis_key: str, data: dict, score: float, encoder=None):
    try:
        json_data = json.dumps(data, cls=encoder)
        await redis_client.zadd(redis_key, {json_data: score})
        await redis_client.publish(channel, json_data)
        print(f"✅ Stored and Published to {channel}")
    except Exception as e:
        print(f"❌ Redis Error ({channel}): {e}")

# Candle state holder
candle_data = {}

def normalize_symbol(symbol: str) -> str:
    return symbol.replace("/", "").replace("-", "").lower()

# TRADE CALLBACK
async def trade_callback(trade, receipt_timestamp):
    global candle_data
    raw_symbol = trade.symbol
    symbol = normalize_symbol(raw_symbol)

    timestamp = int(trade.timestamp)
    candle_timestamp = timestamp // 60 * 60  # minute start

    price = float(trade.price)
    volume = float(trade.amount)
    side = trade.side

    # Store Trade Data
    trade_data = {
        "symbol": raw_symbol,
        "timestamp": datetime.utcfromtimestamp(timestamp).isoformat(),
        "price": price,
        "volume": volume,
        "side": side
    }
    await publish_and_store("realtime:trades", f"trades:{symbol}", trade_data, timestamp)

    # Update or create the current minute's candle
    key = f"candles:{symbol}"
    channel = "realtime:candles"

    if candle_timestamp not in candle_data:
        candle_data[candle_timestamp] = {
            "symbol": raw_symbol,
            "timestamp": datetime.utcfromtimestamp(candle_timestamp),
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": volume
        }
    else:
        candle = candle_data[candle_timestamp]
        candle["high"] = max(candle["high"], price)
        candle["low"] = min(candle["low"], price)
        candle["close"] = price
        candle["volume"] += volume

    # Push the updated candle on every trade
    current_candle = candle_data[candle_timestamp].copy()
    current_candle["timestamp"] = current_candle["timestamp"].isoformat()
    await publish_and_store(channel, key, current_candle, candle_timestamp)

# OPEN INTEREST CALLBACK
async def open_interest_callback(data, receipt_timestamp):
    symbol = normalize_symbol(data.symbol)
    oi_data = {
        "symbol": data.symbol,
        "timestamp": datetime.utcfromtimestamp(data.timestamp).isoformat(),
        "open_interest": float(data.open_interest)
    }
    await publish_and_store("realtime:open_interest", f"open_interest:{symbol}", oi_data, data.timestamp)

# ORDER BOOK CALLBACK
async def order_book_callback(book, receipt_timestamp):
    try:
        symbol = normalize_symbol(book.symbol)
        bids = list(book.book.bids)[:10]
        asks = list(book.book.asks)[:10]

        ob_data = {
            "symbol": book.symbol,
            "timestamp": datetime.utcnow().isoformat(),
            "bids": [(float(price), float(book.book.bids[price])) for price in bids],
            "asks": [(float(price), float(book.book.asks[price])) for price in asks]
        }

        await publish_and_store("realtime:orderbook", f"order_book_snapshots:{symbol}", ob_data, receipt_timestamp, DecimalEncoder)
    except Exception as e:
        print(f"❌ Redis Insert Error (Order Book): {e}")

# FUNDING RATE CALLBACK
async def funding_rate_callback(data, receipt_timestamp):
    symbol = normalize_symbol(data.symbol)
    funding_data = {
        "symbol": data.symbol,
        "timestamp": datetime.utcfromtimestamp(data.timestamp).isoformat(),
        "rate": float(data.rate),
        "interval": getattr(data, 'interval', None)
    }
    await publish_and_store("realtime:funding_rate", f"funding_rate:{symbol}", funding_data, data.timestamp)

# TICKER CALLBACK
async def ticker_callback(data, receipt_timestamp):
    symbol = normalize_symbol(data.symbol)
    ticker_data = {
        "symbol": data.symbol,
        "timestamp": datetime.utcfromtimestamp(data.timestamp).isoformat(),
        "bid": float(data.bid),
        "ask": float(data.ask),
        "last": getattr(data, 'last', None)
    }
    await publish_and_store("realtime:ticker", f"ticker:{symbol}", ticker_data, data.timestamp)

# MAIN FUNCTION
def main():
    f = FeedHandler()
    f.add_feed(BinanceFutures(
        symbols=['BTC-USDT-PERP'],
        channels=[TRADES, OPEN_INTEREST, L2_BOOK, FUNDING, TICKER],
        callbacks={
            TRADES: trade_callback,
            OPEN_INTEREST: open_interest_callback,
            L2_BOOK: order_book_callback,
            FUNDING: funding_rate_callback,
            TICKER: ticker_callback,
        }
    ))
    print("📡 Binance Futures Feed started... waiting for data")
    f.run()

# ENTRY POINT
if __name__ == "__main__":
    if hasattr(asyncio, 'WindowsSelectorEventLoopPolicy'):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    loop.run_until_complete(check_redis_connection())

    main()

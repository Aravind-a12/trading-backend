import logging
import asyncio
from datetime import datetime
import redis
import json
import decimal
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
try:
    redis_client.ping()
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
        redis_client.zadd("trades", {json.dumps(trade_data): timestamp})
        print("✅ Stored trade in Redis")
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

def main():
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
    main()

import logging
import asyncio
from datetime import datetime, timedelta
import os
import redis.asyncio as redis
import json
import decimal
from cryptofeed import FeedHandler
from cryptofeed.exchanges import Binance
from cryptofeed.defines import TRADES, OPEN_INTEREST, L2_BOOK


logging.basicConfig(level=logging.INFO)
logging.getLogger("cryptofeed").setLevel(logging.WARNING)


_redis_host = os.getenv("REDIS_HOST", "127.0.0.1")
_redis_port = int(os.getenv("REDIS_PORT", "6379"))
_redis_password = os.getenv("REDIS_PASSWORD")
redis_client = redis.Redis(
    host=_redis_host,
    port=_redis_port,
    password=_redis_password,
    decode_responses=True,
)


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        return super().default(obj)


INTERVALS = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "1d": 86400,
    "W": 604800,
    "1W": 604800,
    "M": 2592000,  # Note: used only as label; flooring logic will use calendar month
    "1M": 2592000,
}


candle_data = {interval: {} for interval in INTERVALS}


def normalize_symbol(symbol: str) -> str:
    return symbol.replace("/", "").replace("-", "").lower()


def floor_timestamp(timestamp: int, interval_name: str) -> int:
    dt = datetime.utcfromtimestamp(timestamp)
    if interval_name in ['1m', '5m', '15m', '30m', '1h', '1d']:
        interval_sec = INTERVALS[interval_name]
        floored_sec = (timestamp // interval_sec) * interval_sec
        return floored_sec
    elif interval_name in ['W', '1W']:
        # Floor to Monday 00:00:00 UTC of the current week
        start_of_week = dt - timedelta(days=dt.weekday(),
                                       hours=dt.hour,
                                       minutes=dt.minute,
                                       seconds=dt.second,
                                       microseconds=dt.microsecond)
        return int(start_of_week.timestamp())
    elif interval_name in ['M', '1M']:
        # Floor to the first day of the month 00:00:00 UTC
        start_of_month = datetime(year=dt.year, month=dt.month, day=1)
        return int(start_of_month.timestamp())
    else:
        # fallback: no flooring
        return timestamp


async def publish_and_store(channel: str, redis_key: str, data: dict, score: float, encoder=None):
    try:
        json_data = json.dumps(data, cls=encoder)
        await redis_client.zadd(redis_key, {json_data: score})
        await redis_client.publish(channel, json_data)
        print(f"✅ Published to {channel}: {data}")
    except Exception as e:
        print(f"❌ Redis Error ({channel}): {e}")


async def trade_callback(trade, receipt_timestamp):
    raw_symbol = trade.symbol
    symbol = normalize_symbol(raw_symbol)
    timestamp = int(trade.timestamp)
    price = float(trade.price)
    volume = float(trade.amount)

    trade_data = {
        "symbol": raw_symbol,
        "timestamp": datetime.utcfromtimestamp(timestamp).isoformat(),
        "price": price,
        "volume": volume,
        "side": trade.side,
    }

    await publish_and_store(f"realtime:trades", f"trades:{symbol}", trade_data, timestamp)

    for interval_name in INTERVALS:
        ts_floor = floor_timestamp(timestamp, interval_name)
        ts_floor_ms = ts_floor * 1000

        redis_key = f"candles:{symbol}:{interval_name}"
        channel = f"realtime:candles:{symbol}:{interval_name}"

        if ts_floor not in candle_data[interval_name]:
            candle_data[interval_name][ts_floor] = {
                "symbol": raw_symbol,
                "timestamp": ts_floor_ms,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": volume
            }
        else:
            c = candle_data[interval_name][ts_floor]
            c["high"] = max(c["high"], price)
            c["low"] = min(c["low"], price)
            c["close"] = price
            c["volume"] += volume

        current = candle_data[interval_name][ts_floor].copy()
        await publish_and_store(channel, redis_key, current, ts_floor)


async def open_interest_callback(data, ts):
    symbol = normalize_symbol(data.symbol)
    oi = {
        "symbol": data.symbol,
        "timestamp": datetime.utcfromtimestamp(data.timestamp).isoformat(),
        "open_interest": float(data.open_interest)
    }
    await publish_and_store("realtime:open_interest", f"open_interest:{symbol}", oi, data.timestamp)


async def order_book_callback(book, ts):
    symbol = normalize_symbol(book.symbol)
    bids = list(book.book.bids)[:10]
    asks = list(book.book.asks)[:10]
    ob = {
        "symbol": book.symbol,
        "timestamp": datetime.utcnow().isoformat(),
        "bids": [(float(p), float(book.book.bids[p])) for p in bids],
        "asks": [(float(p), float(book.book.asks[p])) for p in asks],
    }
    await publish_and_store("realtime:orderbook", f"order_book_snapshots:{symbol}", ob, ts, DecimalEncoder)


def main():
    f = FeedHandler()
    f.add_feed(Binance(
        symbols=["BTC-USDT"],
        channels=[TRADES, L2_BOOK],
        callbacks={
            TRADES: trade_callback,
            L2_BOOK: order_book_callback
        }
    ))
    print("📡 Binance Spot Feed Started")
    f.run()


if __name__ == "__main__":
    if hasattr(asyncio, 'WindowsSelectorEventLoopPolicy'):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(redis_client.ping())
        print("✅ Redis Connected")
    except Exception as e:
        print(f"❌ Redis Connection Failed: {e}")
        exit(1)

    main()

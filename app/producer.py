import redis
import json
import time
import random

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# Define channels
channels = {
    "trades": "realtime:trades",
    "candles": "realtime:candles",
    "open_interest": "realtime:open_interest",
    "order_book": "realtime:order_book",
    "funding_rate": "realtime:funding_rate",
    "ticker": "realtime:ticker"
}

# Trade data
def generate_trade_data():
    return {
        "price": round(30000 + time.time() % 100, 2),
        "volume": round(1.5 + time.time() % 1, 2),
        "timestamp": int(time.time())
    }

# Candle data
def generate_candle_data():
    return {
        "open": round(30000 + random.uniform(-100, 100), 2),
        "high": round(30000 + random.uniform(50, 150), 2),
        "low": round(30000 + random.uniform(-150, -50), 2),
        "close": round(30000 + random.uniform(-100, 100), 2),
        "timestamp": int(time.time())
    }

# Open interest
def generate_open_interest_data():
    return {
        "open_interest": round(random.uniform(5000, 10000), 2),
        "timestamp": int(time.time())
    }

# Order book
def generate_order_book_data():
    bids = [(round(29950 - i * 5 + random.random(), 2), round(random.uniform(0.1, 2), 3)) for i in range(10)]
    asks = [(round(30050 + i * 5 + random.random(), 2), round(random.uniform(0.1, 2), 3)) for i in range(10)]
    return {
        "timestamp": int(time.time()),
        "bids": bids,
        "asks": asks
    }

# Funding rate
def generate_funding_rate_data():
    return {
        "rate": round(random.uniform(-0.01, 0.01), 6),
        "interval": "8h",
        "timestamp": int(time.time())
    }

# Ticker
def generate_ticker_data():
    bid = round(29990 + random.uniform(-50, 50), 2)
    ask = bid + round(random.uniform(1, 5), 2)
    return {
        "bid": bid,
        "ask": ask,
        "last": round((bid + ask) / 2, 2),
        "timestamp": int(time.time())
    }

# Publisher
def publish_to_redis(channel, data):
    r.publish(channel, json.dumps(data))

# Main loop
while True:
    trade_data = generate_trade_data()
    candle_data = generate_candle_data()
    open_interest_data = generate_open_interest_data()
    order_book_data = generate_order_book_data()
    funding_rate_data = generate_funding_rate_data()
    ticker_data = generate_ticker_data()

    publish_to_redis(channels["trades"], trade_data)
    publish_to_redis(channels["candles"], candle_data)
    publish_to_redis(channels["open_interest"], open_interest_data)
    publish_to_redis(channels["order_book"], order_book_data)
    publish_to_redis(channels["funding_rate"], funding_rate_data)
    publish_to_redis(channels["ticker"], ticker_data)

    print("📤 Published data:")
    print(f"  trades        → {trade_data}")
    print(f"  candles       → {candle_data}")
    print(f"  open_interest → {open_interest_data}")
    print(f"  order_book    → {order_book_data}")
    print(f"  funding_rate  → {funding_rate_data}")
    print(f"  ticker        → {ticker_data}")
    
    time.sleep(2)

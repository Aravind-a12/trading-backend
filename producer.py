# producer.py
import redis
import json
import time

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

channel = "trades"  # or any other channel like "order_book", "open_interest"

while True:
    trade_data = {
        "price": round(30000 + time.time() % 100, 2),
        "volume": round(1.5 + time.time() % 1, 2),
        "timestamp": int(time.time())
    }
    r.zadd(channel, {json.dumps(trade_data): time.time()})
    print(f"📤 Pushed to {channel}: {trade_data}")
    time.sleep(2)

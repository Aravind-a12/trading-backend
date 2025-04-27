import logging
import asyncio
from datetime import datetime
from pymongo import MongoClient
from cryptofeed import FeedHandler
from cryptofeed.exchanges import BinanceFutures
from cryptofeed.defines import TRADES, OPEN_INTEREST

# Setup logging
logging.basicConfig(level=logging.INFO)
logging.getLogger('cryptofeed').setLevel(logging.DEBUG)

# Global candle data storage
candle_data = {}

# MongoDB Configuration
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "binance"
CANDLES_COLLECTION = "candles"
TRADES_COLLECTION = "trades"  # 👈 NEW COLLECTION
OPEN_INTEREST_COLLECTION = "open_interest"

# Connect to MongoDB
try:
    client = MongoClient(MONGO_URI)
    client.server_info()
    print("✅ MongoDB Connected Successfully!")
except Exception as e:
    print(f"❌ MongoDB Connection Error: {e}")
    exit()

# MongoDB Collections
db = client[DB_NAME]
candles_collection = db[CANDLES_COLLECTION]
trades_collection = db[TRADES_COLLECTION]  # 👈 Initialize trades collection
open_interest_collection = db[OPEN_INTEREST_COLLECTION]


async def trade_callback(trade, receipt_timestamp):
    global candle_data
    timestamp = int(trade.timestamp)
    price = float(trade.price)
    volume = float(trade.amount)
    side = trade.side  # buy/sell

    print(f"📌 Trade Data Received - Price: {price}, Volume: {volume}, Side: {side}, Timestamp: {timestamp}")

    # ✅ Store individual trade in MongoDB
    try:
        trades_collection.insert_one({
            "timestamp": datetime.utcfromtimestamp(timestamp).isoformat(),
            "price": price,
            "volume": volume,
            "side": side
        })
        print("✅ Stored individual trade in MongoDB")
    except Exception as e:
        print(f"❌ MongoDB Insert Error (Trade): {e}")

    # ➕ Handle candle aggregation
    if timestamp not in candle_data:
        if candle_data:
            last_timestamp = next(iter(candle_data))
            last_candle = candle_data[last_timestamp]
            last_candle["timestamp"] = last_candle["timestamp"].isoformat()
            try:
                candles_collection.insert_one(last_candle)
                print("✅ Stored candle in MongoDB:", last_candle)
            except Exception as e:
                print(f"❌ MongoDB Insertion Error (Candle): {e}")

        # Start a new candle
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
        # Update current candle
        candle_data[timestamp]["high"] = max(candle_data[timestamp]["high"], price)
        candle_data[timestamp]["low"] = min(candle_data[timestamp]["low"], price)
        candle_data[timestamp]["close"] = price
        candle_data[timestamp]["volume"] += volume


async def open_interest_callback(data, receipt_timestamp):
    open_interest_data = {
        "timestamp": datetime.utcfromtimestamp(data.timestamp).isoformat(),
        "open_interest": float(data.open_interest)
    }

    print(f"📌 Open Interest Data Received: {open_interest_data}")

    try:
        open_interest_collection.insert_one(open_interest_data)
        print("✅ Stored open interest in MongoDB")
    except Exception as e:
        print(f"❌ MongoDB Insertion Error (Open Interest): {e}")


def main():
    f = FeedHandler()

    f.add_feed(BinanceFutures(
        symbols=['BTC-USDT-PERP'],
        channels=[TRADES, OPEN_INTEREST],
        callbacks={
            TRADES: trade_callback,
            OPEN_INTEREST: open_interest_callback
        }
    ))

    print("📡 Binance Futures Feed started... waiting for data")
    f.run()


if __name__ == "__main__":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # For Windows compatibility
    main()

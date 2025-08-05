import asyncio
import websockets
import json
import time
import psycopg2

SPOT_WS = "wss://stream.binance.com:9443/ws/btcusdt@bookTicker"
FUTURES_WS = "wss://fstream.binance.com/ws/btcusdt@bookTicker"

# PostgreSQL connection
conn = psycopg2.connect(
    dbname="orderbook",
    user="postgres",
    password="Sql123#",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

spot_data = {}
futures_data = {}

def store_bookticker(symbol, exchange, bid, bid_qty, ask, ask_qty):
    ts = int(time.time() * 1000)
    total_value = bid * bid_qty + ask * ask_qty
    try:
        cursor.execute(
            "INSERT INTO bookticker_summary (symbol, exchange, best_bid, best_bid_qty, best_ask, best_ask_qty, total_book_value, ts) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (symbol, exchange, bid, bid_qty, ask, ask_qty, total_value, ts)
        )
        conn.commit()
        print(f"✅ Stored {exchange.upper()} BookTicker at {ts}")
    except Exception as e:
        print(f"❌ DB Error: {e}")
        conn.rollback()

async def handle_feed(ws_url, label, storage):
    async with websockets.connect(ws_url) as ws:
        async for msg in ws:
            data = json.loads(msg)
            bid = float(data["b"])
            bid_qty = float(data["B"])
            ask = float(data["a"])
            ask_qty = float(data["A"])
            storage["bid"] = bid
            storage["bid_qty"] = bid_qty
            storage["ask"] = ask
            storage["ask_qty"] = ask_qty

            print_combined_view()

            # Store to DB
            store_bookticker("BTCUSDT", label, bid, bid_qty, ask, ask_qty)

def print_combined_view():
    if spot_data and futures_data:
        print("\n📈 Combined BookTicker View:")
        print(f"🟡 SPOT    - Bid: {spot_data['bid']} ({spot_data['bid_qty']}) | Ask: {spot_data['ask']} ({spot_data['ask_qty']})")
        print(f"🔵 FUTURES - Bid: {futures_data['bid']} ({futures_data['bid_qty']}) | Ask: {futures_data['ask']} ({futures_data['ask_qty']})")
        print("-" * 60)

async def main():
    await asyncio.gather(
        handle_feed(SPOT_WS, "spot", spot_data),
        handle_feed(FUTURES_WS, "futures", futures_data),
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("🛑 Stopped by user.")
        cursor.close()
        conn.close()

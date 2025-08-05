import asyncio
import json
import time
import requests
import psycopg2
import websockets
import signal

SYMBOL = "BTCUSDT"
REST_URL = f"https://fapi.binance.com/fapi/v1/depth?symbol={SYMBOL}&limit=1000"
WS_URL = f"wss://fstream.binance.com/ws/{SYMBOL.lower()}@depth@100ms"

# PostgreSQL connection setup
conn = psycopg2.connect(
    dbname="orderbook",
    user="postgres",
    password="Sql123#",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

# Order book state
bids, asks = {}, {}
last_update_id = 0
_last_stored_ts = 0


def _apply_snapshot_lists(snap_bids, snap_asks):
    bids.clear()
    asks.clear()
    for p, q in snap_bids:
        pf, qf = float(p), float(q)
        if qf > 0:
            bids[pf] = qf
    for p, q in snap_asks:
        pf, qf = float(p), float(q)
        if qf > 0:
            asks[pf] = qf


def _apply_diff(data):
    for p, q in data["b"]:
        pf, qf = float(p), float(q)
        if qf == 0:
            bids.pop(pf, None)
        else:
            bids[pf] = qf
    for p, q in data["a"]:
        pf, qf = float(p), float(q)
        if qf == 0:
            asks.pop(pf, None)
        else:
            asks[pf] = qf


def _publish(update_id):
    global _last_stored_ts

    now_ts = int(time.time())
    if now_ts == _last_stored_ts:
        return

    ts = now_ts * 1000  # store timestamp in ms

    try:
        for price, qty in bids.items():
            cursor.execute(
                "INSERT INTO orderbook_levels (symbol, side, price, quantity, ts) VALUES (%s, %s, %s, %s, %s)",
                (SYMBOL, 'bid', price, qty, ts)
            )
        for price, qty in asks.items():
            cursor.execute(
                "INSERT INTO orderbook_levels (symbol, side, price, quantity, ts) VALUES (%s, %s, %s, %s, %s)",
                (SYMBOL, 'ask', price, qty, ts)
            )
        conn.commit()
        _last_stored_ts = now_ts
        print(f"✅ Stored order book at {now_ts}")
    except Exception as e:
        print(f"❌ DB Error: {e}")
        conn.rollback()


def shutdown():
    print("🛑 Shutdown signal received. Cleaning up...")
    try:
        cursor.close()
        conn.close()
    except:
        pass
    try:
        asyncio.get_event_loop().stop()
    except RuntimeError:
        pass


signal.signal(signal.SIGINT, lambda s, f: shutdown())


async def sync():
    global last_update_id
    print("📥 Connecting to WebSocket and buffering updates...")

    buffer = []

    async with websockets.connect(WS_URL) as ws:
        async def buffer_ws():
            try:
                while True:
                    msg = await ws.recv()
                    buffer.append(json.loads(msg))
            except asyncio.CancelledError:
                pass

        buffer_task = asyncio.create_task(buffer_ws())

        # Fetch snapshot
        print("📥 Fetching initial snapshot...")
        snap = requests.get(REST_URL).json()
        _apply_snapshot_lists(snap["bids"], snap["asks"])
        last_update_id = snap["lastUpdateId"]
        print(f"✅ Snapshot applied. lastUpdateId = {last_update_id}")

        # Stop buffering and apply diff
        buffer_task.cancel()
        try:
            await buffer_task
        except asyncio.CancelledError:
            pass

        print(f"🔄 Applying {len(buffer)} buffered updates...")
        for data in buffer:
            if "u" not in data or "U" not in data:
                continue
            if data["u"] <= last_update_id:
                continue
            if data["U"] > last_update_id:
                _apply_diff(data)
                last_update_id = data["u"]
                _publish(last_update_id)

        print("🚀 Listening to live WebSocket feed...")
        async for msg in ws:
            data = json.loads(msg)
            if "u" not in data or "U" not in data:
                continue
            if data["u"] <= last_update_id:
                continue
            if data["U"] > last_update_id:
                _apply_diff(data)
                last_update_id = data["u"]
                _publish(last_update_id)


async def run_forever():
    while True:
        try:
            await sync()
        except Exception as e:
            print(f"🔁 Restarting due to error: {e}")
            await asyncio.sleep(1)


if __name__ == "__main__":
    try:
        asyncio.run(run_forever())
    except KeyboardInterrupt:
        shutdown()
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        shutdown()

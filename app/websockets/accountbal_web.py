import asyncio
import websockets
import json
import os
from dotenv import load_dotenv
import redis.asyncio as redis

load_dotenv()

WS_BASE = 'wss://stream.binancefuture.com/ws'  

# Redis configuration
redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

async def get_listen_key_from_redis():
    listen_key = await redis_client.get("binance:listen_key")
    if not listen_key:
        raise ValueError("❌ Listen key not found in Redis")
    return listen_key

async def listen_account_updates(listen_key):
    url = f"{WS_BASE}/{listen_key}"
    async with websockets.connect(url) as ws:
        print("✅ Connected to account WebSocket...")
        while True:
            try:
                msg = await ws.recv()
                data = json.loads(msg)

                if data.get("e") == "ACCOUNT_UPDATE":
                    print("📩 ACCOUNT_UPDATE received:")
                    for balance in data["a"]["B"]:
                        print(f"Asset: {balance['a']}, Wallet Balance: {balance['wb']}, Cross Wallet Balance: {balance['cw']}")

                    for pos in data["a"]["P"]:
                        print(f"Symbol: {pos['s']}, Position: {pos['pa']}, Entry Price: {pos['ep']}, Unrealized PnL: {pos['up']}")
                    print('-' * 50)

            except Exception as e:
                print("❌ WebSocket Error:", e)
                break

async def start_listener():
    listen_key = await get_listen_key_from_redis()
    await listen_account_updates(listen_key)

if __name__ == "__main__":
    asyncio.run(start_listener())

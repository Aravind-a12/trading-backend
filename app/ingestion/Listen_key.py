import os
from dotenv import load_dotenv
from urllib.parse import urlencode
import asyncio
import requests
import websockets
import json

# Load variables from .env
load_dotenv()

# Get environment variables
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
FUTURES_BASE_URL = os.getenv("FUTURES_BASE_URL")
LISTEN_KEY_ENDPOINT=os.getenv("LISTEN_KEY_ENDPOINT")

# Stream handling functions
def get_listen_key():
    url = f"{FUTURES_BASE_URL}{LISTEN_KEY_ENDPOINT}"
    headers = {'X-MBX-APIKEY': API_KEY}
    response = requests.post(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        print(f"🔑 ListenKey Created: {data['listenKey']}")
        return data['listenKey']
    else:
        print(f"❌ Error creating listen key: {response.json()}")
        return None

def keep_alive_listen_key(listen_key):
    url = f"{FUTURES_BASE_URL}{LISTEN_KEY_ENDPOINT}"
    headers = {'X-MBX-APIKEY': API_KEY}
    params = {'listenKey': listen_key}
    response = requests.put(url, headers=headers, params=params)
    if response.status_code == 200:
        print("♻️ ListenKey renewed successfully!")
    else:
        print(f"⚠️ Error renewing listen key: {response.json()}")

def delete_listen_key(listen_key):
    url = f"{FUTURES_BASE_URL}{LISTEN_KEY_ENDPOINT}"
    headers = {'X-MBX-APIKEY': API_KEY}
    params = {'listenKey': listen_key}
    response = requests.delete(url, headers=headers, params=params)
    if response.status_code == 200:
        print("🗑️ ListenKey revoked successfully.")
    else:
        print(f"⚠️ Error revoking listen key: {response.json()}")

async def listen_to_user_data_stream(listen_key):
    url = f"wss://fstream.binance.com/ws/{listen_key}"
    async with websockets.connect(url) as websocket:
        print("🔌 Connected to WebSocket!")
        while True:
            try:
                data = await websocket.recv()
                msg = json.loads(data)

                if msg['e'] == 'ACCOUNT_UPDATE':
                    print("💰 Account Balance Update:")
                    for asset in msg['a']['B']:
                        print(f"Asset: {asset['a']}, Wallet Balance: {asset['wb']}, Cross Wallet Balance: {asset['cw']}")
                elif msg['e'] == 'ORDER_TRADE_UPDATE':
                    print("📥 Order Update:")
                    print(json.dumps(msg, indent=2))
                else:
                    print("🔔 Other Event:")
                    print(json.dumps(msg, indent=2))

            except Exception as e:
                print("❌ WebSocket Error:", e)
                break

async def start_user_stream():
    listen_key = get_listen_key()
    if not listen_key:
        print("❌ Failed to get listenKey")
        return

    websocket_task = asyncio.create_task(listen_to_user_data_stream(listen_key))

    try:
        while True:
            await asyncio.sleep(30 * 60)
            keep_alive_listen_key(listen_key)
            print("🔄 ListenKey kept alive.")
    except asyncio.CancelledError:
        print("🛑 Cancelled...")
    finally:
        delete_listen_key(listen_key)
        print("👋 Gracefully shut down.")
        
if __name__ == "__main__":     
    # Now start user stream (non-blocking)
    try:
        asyncio.run(start_user_stream())
    except KeyboardInterrupt:
        print("🧹 Cleanup triggered by KeyboardInterrupt.")
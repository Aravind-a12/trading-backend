import asyncio
import websockets
import requests
import hmac
import hashlib
import time
import json

# Replace with your Binance Futures API key and secret
API_KEY = '5c7c3366d0f9a941e56e93b07a1cf45476dca90478f1e8302397405bb782f5eb'
API_SECRET = '1fde4e8690060be4a6af24ced9c2eab36add4d36092562f2ed2bc222bf41e709'
BASE_URL = 'https://testnet.binancefuture.com'
WS_BASE= 'wss://stream.binancefuture.com/ws'

def get_listen_key():
    url = f"{BASE_URL}/fapi/v1/listenKey"
    headers = {"X-MBX-APIKEY": API_KEY}
    response = requests.post(url, headers=headers)
    return response.json()['listenKey']

async def listen_account_updates(listen_key):
    url = f"{WS_BASE}/{listen_key}"
    async with websockets.connect(url) as ws:
        print("Connected to account stream...")
        while True:
            try:
                msg = await ws.recv()
                data = json.loads(msg)
                
                if data.get('e') == 'ACCOUNT_UPDATE':
                    print("✅ ACCOUNT_UPDATE received:")
                    for balance in data['a']['B']:
                        print(f"Asset: {balance['a']}, Wallet Balance: {balance['wb']}, Cross Wallet Balance: {balance['cw']}")

                    for pos in data['a']['P']:
                        print(f"Symbol: {pos['s']}, Position: {pos['pa']}, Entry Price: {pos['ep']}, Unrealized PnL: {pos['up']}")
                    print('-' * 40)

            except Exception as e:
                print("Error:", e)
                break

def start_listener():
    listen_key = get_listen_key()
    asyncio.run(listen_account_updates(listen_key))

if __name__ == "__main__":
    start_listener()

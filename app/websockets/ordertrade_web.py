import asyncio
import httpx
import websockets
import json

API_KEY = '5c7c3366d0f9a941e56e93b07a1cf45476dca90478f1e8302397405bb782f5eb'
API_SECRET = '1fde4e8690060be4a6af24ced9c2eab36add4d36092562f2ed2bc222bf41e709'
BASE_URL = 'https://testnet.binancefuture.com'
WS_BASE_URL = 'wss://fstream.binancefuture.com/ws'  # use `fstream` for Futures

async def get_listen_key():
    async with httpx.AsyncClient() as client:
        headers = {'X-MBX-APIKEY': API_KEY}
        response = await client.post(f"{BASE_URL}/fapi/v1/listenKey", headers=headers)
        response.raise_for_status()
        return response.json()['listenKey']

async def keep_alive_listen_key(listen_key):
    while True:
        async with httpx.AsyncClient() as client:
            headers = {'X-MBX-APIKEY': API_KEY}
            await client.put(f"{BASE_URL}/fapi/v1/listenKey", headers=headers)
        await asyncio.sleep(30 * 60)  # 30 minutes

async def handle_user_stream(listen_key):
    ws_url = f"{WS_BASE_URL}/{listen_key}"
    async with websockets.connect(ws_url) as ws:
        print("🔌 Connected to Binance Futures WebSocket for user data.")
        async for message in ws:
            data = json.loads(message)
            event_type = data.get("e")

            if event_type == "ORDER_TRADE_UPDATE":
                print("\n📈 ORDER_TRADE_UPDATE received:")
                print(json.dumps(data, indent=4))

async def main():
    listen_key = await get_listen_key()
    await asyncio.gather(
        handle_user_stream(listen_key),
        keep_alive_listen_key(listen_key)
    )

if __name__ == "__main__":
    asyncio.run(main())

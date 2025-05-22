import asyncio
import httpx
import websockets
import json
import logging


API_KEY = '5c7c3366d0f9a941e56e93b07a1cf45476dca90478f1e8302397405bb782f5eb'
API_SECRET = '1fde4e8690060be4a6af24ced9c2eab36add4d36092562f2ed2bc222bf41e709'
BASE_URL = 'https://testnet.binancefuture.com'
WS_BASE_URL = 'wss://stream.binancefuture.com/ws'


async def get_listen_key():
    async with httpx.AsyncClient() as client:
        headers = {'X-MBX-APIKEY': API_KEY}
        response = await client.post(f"{BASE_URL}/fapi/v1/listenKey", headers=headers)
        response.raise_for_status()
        return response.json()['listenKey']


async def keep_alive_listen_key(listen_key):
    while True:
        try:
            async with httpx.AsyncClient() as client:
                headers = {'X-MBX-APIKEY': API_KEY}
                await client.put(f"{BASE_URL}/fapi/v1/listenKey", headers=headers)
            logging.info("🔄 Refreshed listenKey")
        except Exception as e:
            logging.error(f"Failed to refresh listenKey: {e}")
        await asyncio.sleep(1800)  # 30 mins


async def handle_user_stream(listen_key):
    ws_url = f"{WS_BASE_URL}/{listen_key}"
    while True:
        try:
            async with websockets.connect(ws_url) as ws:
                logging.info("🔌 Connected to user data stream.")
                async for message in ws:
                    data = json.loads(message)
                    if data.get("e") == "ACCOUNT_UPDATE":
                        print("\n📥 ACCOUNT_UPDATE received:")
                        print(json.dumps(data, indent=4))
        except websockets.exceptions.ConnectionClosed:
            logging.warning("WebSocket connection closed. Reconnecting...")
            await asyncio.sleep(5)
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            await asyncio.sleep(5)


async def main():
    listen_key = await get_listen_key()
    await asyncio.gather(
        handle_user_stream(listen_key),
        keep_alive_listen_key(listen_key)
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())

import websockets
import asyncio

async def listen():
    url = "ws://localhost:8000/ws/trades"  # Update 'trades' if you want another channel
    async with websockets.connect(url) as websocket:
        print("✅ Connected to WebSocket")
        while True:
            try:
                data = await websocket.recv()
                print(f"📥 Received: {data}")
            except websockets.exceptions.ConnectionClosed:
                print("❌ Connection closed")
                break

if __name__ == "__main__":
    asyncio.run(listen())

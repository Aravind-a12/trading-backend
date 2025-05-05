from fastapi import FastAPI, WebSocket, WebSocketDisconnect, APIRouter
import redis.asyncio as redis
import json
import asyncio

app = FastAPI()
router = APIRouter()
redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

# Map to keep track of connected clients per channel
connected_clients = {}

@router.websocket("/ws/{channel}")
async def websocket_endpoint(websocket: WebSocket, channel: str):
    print(f"hello - WebSocket route hit for channel: {channel}")  # Log that the route is being hit
    
    await websocket.accept()
    print(f"✅ Client connected to: {channel}")

    if channel not in connected_clients:
        connected_clients[channel] = set()
        print(f"hello - No clients in channel {channel}, adding this client.")
    
    connected_clients[channel].add(websocket)
    print(f"hello - Client added to channel {channel}")

    try:
        last_seen = None
        while True:
            print(f"hello - Fetching data for channel: {channel}")  # Log before fetching data
            data = await redis_client.zrevrange(channel, 0, 0)
            print(f"hello - Data fetched from Redis: {data}")  # Log data fetched from Redis

            if data and data[0] != last_seen:
                print(f"hello - Sending data: {data[0]}")  # Log before sending data
                await websocket.send_text(data[0])
                last_seen = data[0]
            await asyncio.sleep(1)

    except WebSocketDisconnect:
        print(f"❌ Client disconnected from {channel}")
        connected_clients[channel].remove(websocket)
    except Exception as e:
        print(f"❌ WebSocket Error ({channel}): {e}")

# Add router to FastAPI app
app.include_router(router)

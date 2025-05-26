from fastapi import FastAPI, APIRouter, WebSocket, WebSocketDisconnect
from starlette.websockets import WebSocketState
import redis.asyncio as redis
import asyncio
import json
from dateutil.parser import isoparse
from asyncio import CancelledError

app = FastAPI()
router = APIRouter()

# Redis async client
redis_client = redis.Redis(host="localhost", port=6379, decode_responses=True)

# Utility to stream Redis pub/sub to WebSocket and convert timestamps
async def stream_channel_to_websocket(websocket: WebSocket, channel_name: str):
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(channel_name)

    try:
        while True:
            try:
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=5.0)

                if message and message["type"] == "message":
                    try:
                        data = json.loads(message["data"])

                        # Convert ISO timestamp to epoch milliseconds if needed
                        if "timestamp" in data:
                            if isinstance(data["timestamp"], str):
                                try:
                                    data["timestamp"] = int(isoparse(data["timestamp"]).timestamp() * 1000)
                                except Exception as e:
                                    print(f"❌ Timestamp parsing error: {e}")
                                    continue
                            elif isinstance(data["timestamp"], (int, float)):
                                data["timestamp"] = int(float(data["timestamp"]) * 1000)

                        # ✅ Check if WebSocket is still open before sending
                        if websocket.application_state == WebSocketState.CONNECTED:
                            await websocket.send_text(json.dumps(data))
                        else:
                            print(f"⚠️ WebSocket closed, stopping stream for {channel_name}")
                            break

                    except Exception as e:
                        print(f"❌ Error processing message: {e}")
                        continue  # Continue on non-fatal error

                await asyncio.sleep(0.01)

            except CancelledError:
                print(f"⚠️ Stream cancelled for {channel_name}")
                break

    except WebSocketDisconnect:
        print(f"🔌 Client disconnected from {channel_name}")
    except Exception as e:
        print(f"❌ Unexpected error in stream: {e}")
    finally:
        await pubsub.unsubscribe(channel_name)
        await pubsub.close()

# WebSocket endpoints
@router.websocket("/ws/candles")
async def websocket_candles(websocket: WebSocket, symbol: str = None):
    await websocket.accept()
    print(f"🔗 WebSocket connected: /ws/candles for symbol={symbol}")
    await stream_channel_to_websocket(websocket, "realtime:candles")

@router.websocket("/ws/trades")
async def websocket_trades(websocket: WebSocket, symbol: str = None):
    await websocket.accept()
    print(f"🔗 WebSocket connected: /ws/trades for symbol={symbol}")
    await stream_channel_to_websocket(websocket, "realtime:trades")

@router.websocket("/ws/open-interest")
async def websocket_open_interest(websocket: WebSocket, symbol: str = None):
    await websocket.accept()
    print(f"🔗 WebSocket connected: /ws/open-interest for symbol={symbol}")
    await stream_channel_to_websocket(websocket, "realtime:open_interest")

@router.websocket("/ws/orderbook")
async def websocket_orderbook(websocket: WebSocket, symbol: str = None):
    await websocket.accept()
    print(f"🔗 WebSocket connected: /ws/orderbook for symbol={symbol}")
    await stream_channel_to_websocket(websocket, "realtime:orderbook")

@router.websocket("/ws/funding-rate")
async def websocket_funding_rate(websocket: WebSocket, symbol: str = None):
    await websocket.accept()
    print(f"🔗 WebSocket connected: /ws/funding-rate for symbol={symbol}")
    await stream_channel_to_websocket(websocket, "realtime:funding_rate")

@router.websocket("/ws/ticker")
async def websocket_ticker(websocket: WebSocket, symbol: str = None):
    await websocket.accept()
    print(f"🔗 WebSocket connected: /ws/ticker for symbol={symbol}")
    await stream_channel_to_websocket(websocket, "realtime:ticker")

# Include router
app.include_router(router)

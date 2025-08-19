from fastapi import FastAPI, APIRouter, WebSocket, WebSocketDisconnect
from starlette.websockets import WebSocketState
import os
import redis.asyncio as redis
import asyncio
import json
from dateutil.parser import isoparse
from asyncio import CancelledError

app = FastAPI()
router = APIRouter()

# Redis async client from env
_redis_host = os.getenv("REDIS_HOST", "127.0.0.1")
_redis_port = int(os.getenv("REDIS_PORT", "6379"))
_redis_password = os.getenv("REDIS_PASSWORD")
redis_client = redis.Redis(
    host=_redis_host,
    port=_redis_port,
    password=_redis_password,
    decode_responses=True,
)

def normalize_symbol(raw: str | None) -> str | None:
    if not raw:
        return None
    return raw.replace("/", "").replace("-", "").lower()

async def stream_channel_to_websocket(websocket: WebSocket, channel: str):
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(channel)
    try:
        while True:
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=5.0)
            if message and message["type"] == "message":
                try:
                    data = json.loads(message["data"])
                    if "timestamp" in data:
                        if isinstance(data["timestamp"], str):
                            data["timestamp"] = int(isoparse(data["timestamp"]).timestamp() * 1000)
                        elif isinstance(data["timestamp"], (int, float)):
                            data["timestamp"] = int(float(data["timestamp"]) * 1000)

                    if websocket.application_state == WebSocketState.CONNECTED:
                        print("📤 Sending to WebSocket:", data)
                        await websocket.send_text(json.dumps(data))
                    else:
                        break
                except Exception as e:
                    print(f"❌ Data parse error: {e}")
            await asyncio.sleep(0.01)
    except WebSocketDisconnect:
        print("🔌 Client disconnected")
    finally:
        await pubsub.unsubscribe(channel)
        await pubsub.close()



@router.websocket("/ws/candles")
async def websocket_candles(websocket: WebSocket):
    await websocket.accept()
    symbol = websocket.query_params.get("symbol")
    interval = websocket.query_params.get("interval")

    if not symbol or not interval:
        await websocket.close()
        print("❌ Missing symbol or interval")
        return

    # Normalize to match publisher format (e.g., BTC-USDT -> btcusdt)
    normalized_symbol = symbol.replace("/", "").replace("-", "").lower()
    redis_channel = f"realtime:candles:{normalized_symbol}:{interval}"
    print(f"🔗 WS connected: {redis_channel}")
    await stream_channel_to_websocket(websocket, redis_channel)



@router.websocket("/ws/trades")
async def websocket_trades(websocket: WebSocket, symbol: str = None):
    await websocket.accept()
    raw_symbol = websocket.query_params.get("symbol") or symbol
    sym = normalize_symbol(raw_symbol)
    channel = f"realtime:trades:{sym}" if sym else "realtime:trades"
    print(f"🔗 WebSocket connected: /ws/trades channel={channel}")
    await stream_channel_to_websocket(websocket, channel)

@router.websocket("/ws/open-interest")
async def websocket_open_interest(websocket: WebSocket, symbol: str = None):
    await websocket.accept()
    raw_symbol = websocket.query_params.get("symbol") or symbol
    sym = normalize_symbol(raw_symbol)
    channel = f"realtime:open_interest:{sym}" if sym else "realtime:open_interest"
    print(f"🔗 WebSocket connected: /ws/open-interest channel={channel}")
    await stream_channel_to_websocket(websocket, channel)

@router.websocket("/ws/orderbook")
async def websocket_orderbook(websocket: WebSocket, symbol: str = None):
    await websocket.accept()
    raw_symbol = websocket.query_params.get("symbol") or symbol
    sym = normalize_symbol(raw_symbol)
    channel = f"realtime:orderbook:{sym}" if sym else "realtime:orderbook"
    print(f"🔗 WebSocket connected: /ws/orderbook channel={channel}")
    await stream_channel_to_websocket(websocket, channel)

@router.websocket("/ws/funding-rate")
async def websocket_funding_rate(websocket: WebSocket, symbol: str = None):
    await websocket.accept()
    raw_symbol = websocket.query_params.get("symbol") or symbol
    sym = normalize_symbol(raw_symbol)
    channel = f"realtime:funding_rate:{sym}" if sym else "realtime:funding_rate"
    print(f"🔗 WebSocket connected: /ws/funding-rate channel={channel}")
    await stream_channel_to_websocket(websocket, channel)

@router.websocket("/ws/ticker")
async def websocket_ticker(websocket: WebSocket, symbol: str = None):
    await websocket.accept()
    raw_symbol = websocket.query_params.get("symbol") or symbol
    sym = normalize_symbol(raw_symbol)
    channel = f"realtime:ticker:{sym}" if sym else "realtime:ticker"
    print(f"🔗 WebSocket connected: /ws/ticker channel={channel}")
    await stream_channel_to_websocket(websocket, channel)

# Include router
app.include_router(router)
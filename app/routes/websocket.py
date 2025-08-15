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

async def stream_candles_from_sortedset(websocket: WebSocket, key: str):
    last_score = None
    try:
        while True:
            # Get the latest candle
            data_list = await redis_client.zrevrange(key, 0, 0, withscores=True)
            if data_list:
                raw_json, score = data_list[0]
                if score != last_score:
                    last_score = score
                    data = json.loads(raw_json)
                    if isinstance(data["timestamp"], (int, float)) and data["timestamp"] < 10**12:
                        data["timestamp"] = int(data["timestamp"]) * 1000
                    await websocket.send_text(json.dumps(data))
            await asyncio.sleep(1)  # check every second
    except WebSocketDisconnect:
        pass

@router.websocket("/ws/candles")
async def websocket_candles(websocket: WebSocket):
    await websocket.accept()
    symbol = websocket.query_params.get("symbol")
    interval = websocket.query_params.get("interval")
    if not symbol or not interval:
        await websocket.close()
        return
    key = f"candles:{symbol}:{interval}"
    await stream_candles_from_sortedset(websocket, key)




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
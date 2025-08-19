from fastapi import FastAPI, APIRouter, WebSocket, WebSocketDisconnect
import os
import redis.asyncio as redis
import asyncio
import json

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


# -----------------------------
# Helper: Candle stream (from sorted set)
# -----------------------------
async def stream_candles_from_sortedset(websocket: WebSocket, key: str):
    last_score = None
    try:
        while True:
            data_list = await redis_client.zrevrange(key, 0, 0, withscores=True)
            if data_list:
                raw_json, score = data_list[0]
                if score != last_score:
                    last_score = score
                    data = json.loads(raw_json)

                    # normalize timestamp if needed
                    if isinstance(data.get("timestamp"), (int, float)) and data["timestamp"] < 10**12:
                        data["timestamp"] = int(data["timestamp"]) * 1000

                    await websocket.send_text(json.dumps(data))

            # heartbeat to keep connection alive
            await websocket.send_text(json.dumps({"type": "ping"}))
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        print("❌ WebSocket disconnected: candles")
    except Exception as e:
        print(f"⚠️ Candle stream error: {e}")


# -----------------------------
# Helper: Generic pub/sub stream
# -----------------------------
async def stream_channel_to_websocket(websocket: WebSocket, channel: str):
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(channel)

    try:
        async for message in pubsub.listen():
            if message["type"] == "message":
                data = message["data"]
                await websocket.send_text(data)
    except WebSocketDisconnect:
        print(f"❌ WebSocket disconnected from {channel}")
        await pubsub.unsubscribe(channel)
    except Exception as e:
        print(f"⚠️ Error in {channel} stream: {e}")
        try:
            await pubsub.unsubscribe(channel)
        except:
            pass


# -----------------------------
# Routes
# -----------------------------
@router.websocket("/ws/candles")
async def websocket_candles(websocket: WebSocket):
    await websocket.accept()
    symbol = websocket.query_params.get("symbol")
    interval = websocket.query_params.get("interval")
    if not symbol or not interval:
        await websocket.close()
        return
    key = f"candles:{symbol}:{interval}"
    print(f"🔗 WebSocket connected: /ws/candles {symbol} {interval}")
    await stream_candles_from_sortedset(websocket, key)


@router.websocket("/ws/trades")
async def websocket_trades(websocket: WebSocket):
    await websocket.accept()
    print("🔗 WebSocket connected: /ws/trades")
    await stream_channel_to_websocket(websocket, "realtime:trades")


@router.websocket("/ws/orderbook")
async def websocket_orderbook(websocket: WebSocket):
    await websocket.accept()
    print("🔗 WebSocket connected: /ws/orderbook")
    await stream_channel_to_websocket(websocket, "realtime:orderbook")


@router.websocket("/ws/open-interest")
async def websocket_open_interest(websocket: WebSocket):
    await websocket.accept()
    print("🔗 WebSocket connected: /ws/open-interest")
    await stream_channel_to_websocket(websocket, "realtime:open_interest")


@router.websocket("/ws/funding-rate")
async def websocket_funding_rate(websocket: WebSocket):
    await websocket.accept()
    print("🔗 WebSocket connected: /ws/funding-rate")
    await stream_channel_to_websocket(websocket, "realtime:funding_rate")


@router.websocket("/ws/ticker")
async def websocket_ticker(websocket: WebSocket):
    await websocket.accept()
    print("🔗 WebSocket connected: /ws/ticker")
    await stream_channel_to_websocket(websocket, "realtime:ticker")


# -----------------------------
# Include router
# -----------------------------
app.include_router(router)

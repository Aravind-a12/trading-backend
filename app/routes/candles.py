from fastapi import APIRouter
from app.utils.redis_client import redis_client
import json

router = APIRouter()

@router.get("/")
def get_latest_candles(limit: int = 10):
    candles = redis_client.zrevrange("candles", -limit, -1)
    return [json.loads(candle) for candle in candles]

@router.get("/range")
def get_candles_in_range(start_ts: int, end_ts: int):
    candles = redis_client.zrangebyscore("candles", start_ts, end_ts)
    return [json.loads(candle) for candle in candles]

# ✅ Paginate using index
@router.get("/paginate")
def paginate_candles(start: int = 0, end: int = 9):
    candles = redis_client.zrevrange("candles", start, end)
    return [json.loads(c) for c in candles]

from fastapi import APIRouter
from app.utils.redis_client import redis_client
import json

router = APIRouter()

# Fetch the latest trades data
@router.get("/")
def get_latest_trades(limit: int = 10):
    trades = redis_client.zrevrange("trades", -limit, -1)
    return [json.loads(trade) for trade in trades]

# Fetch trades within a specific timestamp range
@router.get("/range")
def get_trades_in_range(start_ts: int, end_ts: int):
    trades = redis_client.zrangebyscore("trades", start_ts, end_ts)
    return [json.loads(trade) for trade in trades]

# Paginate the trades data
@router.get("/paginate")
def paginate_trades(start: int = 0, end: int = 9):
    trades = redis_client.zrevrange("trades", start, end)
    return [json.loads(trade) for trade in trades]

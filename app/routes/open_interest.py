from fastapi import APIRouter
from app.utils.redis_client import redis_client
import json

router = APIRouter()

# Fetch the latest open interest data
@router.get("/")
def get_latest_open_interest(limit: int = 10):
    oi = redis_client.zrevrange("open_interest", -limit, -1)
    return [json.loads(entry) for entry in oi]

# Fetch open interest data within a specific timestamp range
@router.get("/range")
def get_open_interest_in_range(start_ts: int, end_ts: int):
    oi = redis_client.zrangebyscore("open_interest", start_ts, end_ts)
    return [json.loads(entry) for entry in oi]

# Paginate the open interest data
@router.get("/paginate")
def paginate_open_interest(start: int = 0, end: int = 9):
    oi = redis_client.zrevrange("open_interest", start, end)
    return [json.loads(entry) for entry in oi]

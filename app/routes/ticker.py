from fastapi import APIRouter
from app.utils.redis_client import redis_client
import json

router = APIRouter()

# Get the latest ticker data (default: 10 latest entries)
@router.get("/ticker")
def get_latest_ticker(limit: int = 10):
    ticker = redis_client.zrevrange("ticker", -limit, -1)
    return [json.loads(entry) for entry in ticker]

# Get ticker data within a specific timestamp range
@router.get("/ticker/range")
def get_ticker_in_range(start_ts: int, end_ts: int):
    ticker = redis_client.zrangebyscore("ticker", start_ts, end_ts)
    return [json.loads(entry) for entry in ticker]

# Paginate through ticker data using indexes (default: from index 0 to 9)
@router.get("/ticker/paginate")
def paginate_ticker(start: int = 0, end: int = 9):
    ticker = redis_client.zrevrange("ticker", start, end)
    return [json.loads(entry) for entry in ticker]

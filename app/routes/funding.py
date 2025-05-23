from fastapi import APIRouter
from app.utils.redis_client import redis_client
import json

router = APIRouter()

@router.get("/funding")
def get_latest_funding(limit: int = 10):
    funding_data = redis_client.zrevrange("funding", -limit, -1)
    return [json.loads(entry) for entry in funding_data]

@router.get("/funding/range")
def get_funding_in_range(start_ts: int, end_ts: int):
    funding_data = redis_client.zrangebyscore("funding", start_ts, end_ts)
    return [json.loads(entry) for entry in funding_data]

@router.get("/funding/paginate")
def paginate_funding(start: int = 0, end: int = 9):
    funding_data = redis_client.zrevrange("funding", start, end)
    return [json.loads(entry) for entry in funding_data]
  
  

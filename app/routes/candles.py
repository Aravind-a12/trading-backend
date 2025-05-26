from fastapi import APIRouter, HTTPException, Query
from app.utils.redis_client import redis_client
import json
import logging

logger = logging.getLogger("uvicorn.error")

router = APIRouter()

@router.get("/")
def get_latest_candles(limit: int = 10):
    candles = redis_client.zrevrange("candles", 0, limit - 1)
    return [json.loads(candle) for candle in candles]

@router.get("/range")
def get_candles_in_range(
    symbol: str = Query(...),
    from_: int = Query(..., alias="from"),
    to_: int = Query(..., alias="to"),
    resolution: str = Query(None)
):
    if from_ > to_:
        raise HTTPException(status_code=400, detail="from must be <= to")

    redis_key = f"candles:{symbol}"
    try:
        candles = redis_client.zrangebyscore(redis_key, from_, to_)
        parsed_candles = [json.loads(candle) for candle in candles]
        return parsed_candles
    except Exception as e:
        logger.error(f"Error in get_candles_in_range: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# Paginate candles by index (useful for frontend paging)
@router.get("/paginate")
def paginate_candles(start: int = 0, end: int = 9):
    candles = redis_client.zrevrange("candles", start, end)
    return [json.loads(c) for c in candles]

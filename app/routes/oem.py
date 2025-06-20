from fastapi import APIRouter, HTTPException,Query
from app.utils.redis_client import redis_client
import json

router = APIRouter()
REDIS_KEY = "oem_device_logs"

@router.get("/")
def get_latest_oem_logs(limit: int = 10):
    try:
        oem_logs = redis_client.lrange(REDIS_KEY, 0, limit - 1)
        return [json.loads(oem_log) for oem_log in oem_logs]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Redis Error: {e}")

@router.get("/range")
def get_oem_logs_in_range(start_ts: int, end_ts: int):
    try:
        oem_logs = redis_client.lrange(REDIS_KEY, 0, -1)
        return [
            json.loads(log)
            for log in oem_logs
            if start_ts <= int(json.loads(log)["timestamp"]) <= end_ts
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error filtering logs: {e}")

@router.get("/paginate")
def paginate_oem_logs(start: int = 0, end: int = 9):
    try:
        oem_logs = redis_client.lrange(REDIS_KEY, start, end)
        return [json.loads(oem_log) for oem_log in oem_logs]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Redis Error: {e}")
    
@router.get("/symbol")
def get_oem_logs_by_symbol(
    symbol: str = Query(..., description="Trading symbol like BTCUSDT")
):
    try:
        oem_logs = redis_client.lrange(REDIS_KEY, 0, -1)
        filtered_logs = [
            json.loads(log)
            for log in oem_logs
            if json.loads(log).get("symbol") == symbol
        ]
        return filtered_logs
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Redis Error: {e}")
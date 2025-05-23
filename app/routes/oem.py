from fastapi import APIRouter
from app.utils.redis_client import redis_client
import json

router = APIRouter()

@router.get("/")
def get_latest_oem_logs(limit: int = 10):
    oem_logs = redis_client.lrange("oem_logs", 0, limit - 1)
    return [json.loads(oem_log) for oem_log in oem_logs]

@router.get("/range")
def get_oem_logs_in_range(start_ts: int, end_ts: int):
    # OEM logs are typically stored as a list, so you might need to adjust depending on how they are stored
    oem_logs = redis_client.lrange("oem_logs", 0, -1)  # Get all and filter by timestamp
    oem_logs_in_range = [json.loads(log) for log in oem_logs if start_ts <= int(json.loads(log)["timestamp"]) <= end_ts]
    return oem_logs_in_range

# ✅ Paginate using index
@router.get("/paginate")
def paginate_oem_logs(start: int = 0, end: int = 9):
    oem_logs = redis_client.lrange("oem_logs", start, end)
    return [json.loads(oem_log) for oem_log in oem_logs]

from fastapi import APIRouter, HTTPException
from app.ingestion.exchange_info import store_futures_exchange_info,store_spot_exchange_info

router = APIRouter()

@router.get("/")
async def future_exchange_info():
    try:
        info = store_futures_exchange_info()
        return info
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/spot")
async def spot_exchange_info():
    try:
        info = store_spot_exchange_info()
        return info
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

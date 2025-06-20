from fastapi import APIRouter, HTTPException,Query
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

@router.get("/symbol")
async def get_futures_exchange_info_by_symbol(
    symbol: str = Query(..., description="Trading symbol like BTCUSDT")
):
    try:
        info = store_futures_exchange_info()
        if isinstance(info, list):
            filtered_info = [item for item in info if item.get("symbol") == symbol]
            return filtered_info or {"message": "Symbol not found"}
        return {"message": "Unexpected data format"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/spot/symbol")
async def get_spot_exchange_info_by_symbol(
    symbol: str = Query(..., description="Spot trading symbol like BTCUSDT")
):
    try:
        info = store_spot_exchange_info()
        if isinstance(info, list):
            filtered_info = [item for item in info if item.get("symbol") == symbol]
            return filtered_info or {"message": "Symbol not found"}
        return {"message": "Unexpected data format"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
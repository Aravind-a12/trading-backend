from fastapi import APIRouter, HTTPException,Query
from app.ingestion.user_data import get_open_orders

router = APIRouter()

@router.get("/")
async def get_openorders(symbol: str = Query(None, description="Trading symbol, e.g. BTCUSDT")):
    try:
        open_orders=get_open_orders(symbol)
        return open_orders
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
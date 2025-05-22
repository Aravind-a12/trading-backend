from fastapi import APIRouter, HTTPException,Query
from app.ingestion.user_data import get_all_the_orders
import os 

router = APIRouter()

@router.get("/orders")
async def get_allorders(symbol: str = Query(..., description="Trading symbol, e.g. BTCUSDT")):
    try:
        orders=get_all_the_orders(symbol)
        return orders
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
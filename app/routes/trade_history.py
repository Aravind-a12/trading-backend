from fastapi import APIRouter, HTTPException, Query
from app.ingestion.user_data import get_trade_history  

router = APIRouter()

@router.get("/trade-history")
async def get_trade_history_api(symbol: str = Query(..., description="Trading symbol, e.g. BTCUSDT")):
    try:
        trades = get_trade_history(symbol)
        return trades
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
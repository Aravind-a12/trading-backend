from fastapi import APIRouter,HTTPException
from app.ingestion.binance_feed import get_aggregate_trades_futures,get_aggregate_trades_spot

router = APIRouter()

@router.get("/{symbol}")
async def get_agg_trades(symbol: str):
    try:
        agg_trades = get_aggregate_trades_futures(symbol, limit=5)
        return agg_trades  # returns only aggtrades
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/spot/{symbol}")
async def get_agg_trades_spot(symbol: str):
    try:
        agg_trades = get_aggregate_trades_spot(symbol, limit=5)
        return agg_trades
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
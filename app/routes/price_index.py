from fastapi import APIRouter, HTTPException, Query
from app.ingestion.price_index import get_margin_price_index

router = APIRouter(prefix="/api/price_index", tags=["price_index"])

@router.get("/")
async def price_index(symbol: str = Query(..., description="Margin-enabled pair, e.g. BNBBTC")):
    try:
        return get_margin_price_index(symbol)
    except Exception as exc:
        # send Binance's own message to the caller
        raise HTTPException(status_code=400, detail=str(exc))

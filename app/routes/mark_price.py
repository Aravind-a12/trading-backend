from fastapi import APIRouter, HTTPException, Query
from app.ingestion.mark_price import get_mark_price

router = APIRouter()

@router.get("/")
async def mark_price(
    symbol: str | None = Query(
        None,
        description="Optional trading pair, e.g. BTCUSDT. "
                    "If omitted returns all symbols."
    )
):
    try:
        return get_mark_price(symbol)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

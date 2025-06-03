from fastapi import APIRouter, HTTPException, Query

from app.ingestion.symbol_price_ticker import (
    get_futures_ticker,
    get_spot_ticker,
    symbol as DEFAULT_SYMBOL,   # import default
)

router = APIRouter(prefix="/api/ticker", tags=["ticker"])

# ────────────────────────── FUTURES ───────────────────────────────────────
@router.get("/futures")
async def futures_ticker(
    symbol: str | None = Query(
        None,
        description=f"Trading pair (default {DEFAULT_SYMBOL})"
    )
):
    try:
        return get_futures_ticker(symbol)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

# ────────────────────────── SPOT ──────────────────────────────────────────
@router.get("/spot")
async def spot_ticker(
    symbol: str | None = Query(
        None,
        description=f"Trading pair (default {DEFAULT_SYMBOL})"
    )
):
    try:
        return get_spot_ticker(symbol)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

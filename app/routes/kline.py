from fastapi import APIRouter, HTTPException
from app.ingestion.kline import get_klines_futures, get_klines_spot, get_klines_futures_for_day, get_klines_spot_for_day

router = APIRouter()

@router.get("/day/{symbol}")
async def get_klines_for_day(symbol: str, interval: str = "1m"):
    try:
        raw_klines = get_klines_futures_for_day(symbol, interval)
        return raw_klines
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/spot/day/{symbol}")
async def get_klines_spot_for_day_route(symbol: str, interval: str = "1m"):
    try:
        raw_klines = get_klines_spot_for_day(symbol, interval)
        return raw_klines
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{symbol}")
async def get_klines(symbol: str, interval: str = "1m", limit: int = 1000, startTime: int = None, endTime: int = None):
    try:
        # Convert from seconds (TradingView) to milliseconds (Binance) if provided
        start_ms = startTime * 1000 if startTime else None
        end_ms = endTime * 1000 if endTime else None
        raw_klines = get_klines_futures(symbol, interval, limit, start_time=start_ms, end_time=end_ms)
        return raw_klines
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/spot/{symbol}")
async def get_klines_spot_route(symbol: str, interval: str = "1m", limit: int = 1000, startTime: int = None, endTime: int = None):
    try:
        # Convert from seconds (TradingView) to milliseconds (Binance) if provided
        start_ms = startTime * 1000 if startTime else None
        end_ms = endTime * 1000 if endTime else None
        raw_klines = get_klines_spot(symbol, interval, limit, start_time=start_ms, end_time=end_ms)
        return raw_klines
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

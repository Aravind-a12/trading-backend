from fastapi import APIRouter, HTTPException
from app.ingestion.binance_feed import get_klines_futures, get_klines_spot

router = APIRouter()

@router.get("/{symbol}")
async def get_klines(symbol: str, interval: str = "1m", limit: int = 5):
    try:
        raw_klines = get_klines_futures(symbol, interval, limit)
        formatted_klines = [
            {
                "Symbol": symbol.upper(),
                "Interval": interval,
                "Open": k[1],
                "High": k[2],
                "Low": k[3],
                "Close": k[4],
                "Volume": k[5],
                "Start Time": k[0],
                "End Time": k[6]
            }
            for k in raw_klines
        ]
        return formatted_klines
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/spot/{symbol}")
async def get_klines_spot_route(symbol: str, interval: str = "1m", limit: int = 5):
    try:
        raw_klines = get_klines_spot(symbol, interval, limit)  # remember this is sync, so no await
        formatted_klines = [
            {
                "Symbol": symbol.upper(),
                "Interval": interval,
                "Open": k[1],
                "High": k[2],
                "Low": k[3],
                "Close": k[4],
                "Volume": k[5],
                "Start Time": k[0],
                "End Time": k[6]
            }
            for k in raw_klines
        ]
        return formatted_klines
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

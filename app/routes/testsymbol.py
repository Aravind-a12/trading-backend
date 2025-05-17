from fastapi import APIRouter, HTTPException
from app.ingestion.binance_feed import (
    store_futures_exchange_info,
    get_aggregate_trades,
    get_klines,
)

router = APIRouter()

@router.get("/aggtrades/{symbol}")
async def get_agg_trades(symbol: str):
    try:
        agg_trades = get_aggregate_trades(symbol, limit=5)
        return agg_trades  # returns only aggtrades
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/klines/{symbol}")
async def get_klines_data(symbol: str, interval: str = "1m", limit: int = 5):
    try:
        raw_klines = get_klines(symbol, interval=interval, limit=limit)
        
        formatted_klines = [
            {
                "Symbol": symbol,
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



# Aggregate Trades:
# http://127.0.0.1:8000/api/symbol/aggtrades/BTCUSDT

# Klines (Candlesticks):
# http://127.0.0.1:8000/api/symbol/klines/BTCUSDT


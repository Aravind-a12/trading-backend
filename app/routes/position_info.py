from fastapi import APIRouter, HTTPException, Query
from app.ingestion.user_data import get_position_risk  

router = APIRouter()

@router.get("/position-risk")
async def get_position_risk_api(symbol: str = Query(None, description="Trading symbol, e.g. BTCUSDT")):
    try:
        positions = get_position_risk(symbol)
        return positions
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
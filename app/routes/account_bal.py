from fastapi import APIRouter, HTTPException,Query
from app.ingestion.user_data import get_futures_account_balance_v3

router = APIRouter()

@router.get("/balance")
async def get_acc_balance():
    try:
        acc_bal=get_futures_account_balance_v3()
        return acc_bal
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/balance/symbol")
async def get_balance_by_symbol(
    symbol: str = Query(..., description="Trading symbol like BTCUSDT or asset like USDT")
):
    try:
        acc_bal = get_futures_account_balance_v3()
        if isinstance(acc_bal, list):
            filtered = [item for item in acc_bal if item.get("symbol") == symbol or item.get("asset") == symbol]
            return filtered or {"message": "No balance found for the given symbol"}
        return {"message": "Unexpected response format"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
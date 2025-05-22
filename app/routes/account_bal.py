from fastapi import APIRouter, HTTPException
from app.ingestion.user_data import get_futures_account_balance_v3
import os 

router = APIRouter()

@router.get("/balance")
async def get_acc_balance():
    try:
        acc_bal=get_futures_account_balance_v3()
        return acc_bal
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
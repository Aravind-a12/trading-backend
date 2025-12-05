from fastapi import APIRouter, Depends, HTTPException, status
from typing import Dict, Any
from app.utils.redis_auth import get_current_active_user
from app.utils.data_fetcher import fetch_all_trading_data
import json

router = APIRouter()

@router.get("/{api_key_id}/all-data", response_model=Dict[str, Any])
async def get_all_trading_data(
    api_key_id: str,
    current_user: dict = Depends(get_current_active_user)
):
    """Fetch ALL trading data from the exchange API key."""
    try:
        all_data = await fetch_all_trading_data(api_key_id, current_user["id"])
        
        if "error" in all_data:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=all_data["error"]
            )
        
        return all_data
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching trading data: {str(e)}"
        )

@router.get("/{api_key_id}/account-summary")
async def get_account_summary(
    api_key_id: str,
    current_user: dict = Depends(get_current_active_user)
):
    """Get a summary of account data."""
    try:
        all_data = await fetch_all_trading_data(api_key_id, current_user["id"])
        
        if "error" in all_data:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=all_data["error"]
            )
        
        # Create summary
        summary = {
            "exchange": all_data.get("exchange"),
            "timestamp": all_data.get("timestamp"),
            "account_info": all_data.get("account_info", {}),
            "total_balances": len(all_data.get("balances", [])),
            "open_orders_count": len(all_data.get("open_orders", [])),
            "order_history_count": len(all_data.get("order_history", [])),
            "trade_history_count": len(all_data.get("trade_history", [])),
            "active_positions_count": len(all_data.get("positions", [])),
            "total_balance_usdt": 0,
            "sample_balances": all_data.get("balances", [])[:5],  # First 5 balances
            "recent_orders": all_data.get("order_history", [])[:5],  # First 5 orders
            "recent_trades": all_data.get("trade_history", [])[:5],  # First 5 trades
            "active_positions": all_data.get("positions", [])[:5]  # First 5 positions
        }
        
        # Calculate total USDT balance
        for balance in all_data.get("balances", []):
            if balance.get("asset") == "USDT":
                summary["total_balance_usdt"] = float(balance.get("balance", 0))
                break
        
        return summary
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching account summary: {str(e)}"
        )

@router.get("/{api_key_id}/balances")
async def get_balances(
    api_key_id: str,
    current_user: dict = Depends(get_current_active_user)
):
    """Get all account balances."""
    try:
        all_data = await fetch_all_trading_data(api_key_id, current_user["id"])
        
        if "error" in all_data:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=all_data["error"]
            )
        
        return {
            "exchange": all_data.get("exchange"),
            "balances": all_data.get("balances", []),
            "total_assets": len(all_data.get("balances", []))
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching balances: {str(e)}"
        )

@router.get("/{api_key_id}/orders")
async def get_orders(
    api_key_id: str,
    current_user: dict = Depends(get_current_active_user)
):
    """Get all orders (open and history)."""
    try:
        all_data = await fetch_all_trading_data(api_key_id, current_user["id"])
        
        if "error" in all_data:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=all_data["error"]
            )
        
        return {
            "exchange": all_data.get("exchange"),
            "open_orders": all_data.get("open_orders", []),
            "order_history": all_data.get("order_history", []),
            "open_orders_count": len(all_data.get("open_orders", [])),
            "total_orders_count": len(all_data.get("order_history", []))
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching orders: {str(e)}"
        )

@router.get("/{api_key_id}/trades")
async def get_trades(
    api_key_id: str,
    current_user: dict = Depends(get_current_active_user)
):
    """Get all trade history."""
    try:
        all_data = await fetch_all_trading_data(api_key_id, current_user["id"])
        
        if "error" in all_data:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=all_data["error"]
            )
        
        return {
            "exchange": all_data.get("exchange"),
            "trade_history": all_data.get("trade_history", []),
            "total_trades": len(all_data.get("trade_history", []))
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching trades: {str(e)}"
        )

@router.get("/{api_key_id}/positions")
async def get_positions(
    api_key_id: str,
    current_user: dict = Depends(get_current_active_user)
):
    """Get all active positions."""
    try:
        all_data = await fetch_all_trading_data(api_key_id, current_user["id"])
        
        if "error" in all_data:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=all_data["error"]
            )
        
        return {
            "exchange": all_data.get("exchange"),
            "positions": all_data.get("positions", []),
            "active_positions_count": len(all_data.get("positions", []))
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching positions: {str(e)}"
        )

@router.get("/{api_key_id}/market-data")
async def get_market_data(
    api_key_id: str,
    current_user: dict = Depends(get_current_active_user)
):
    """Get market data and statistics."""
    try:
        all_data = await fetch_all_trading_data(api_key_id, current_user["id"])
        
        if "error" in all_data:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=all_data["error"]
            )
        
        return {
            "exchange": all_data.get("exchange"),
            "ticker_24hr": all_data.get("ticker_24hr", []),
            "open_interest": all_data.get("open_interest", []),
            "long_short_ratio": all_data.get("long_short_ratio", []),
            "funding_rate": all_data.get("funding_rate", []),
            "exchange_info": all_data.get("exchange_info", {})
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching market data: {str(e)}"
        )

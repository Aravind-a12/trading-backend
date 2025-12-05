from fastapi import APIRouter, Depends, HTTPException
from typing import Dict, Any, List, Optional
from pydantic import BaseModel
from app.utils.redis_auth import get_current_active_user
from app.utils.redis_api_keys import get_user_api_keys
import numpy as np
from datetime import datetime, timedelta

router = APIRouter()

class RiskLimits(BaseModel):
    max_position_size: float
    max_portfolio_risk: float  # Max % of portfolio at risk
    max_drawdown: float
    max_leverage: float
    max_correlation: float
    stop_loss_percentage: float

class PositionSizingRequest(BaseModel):
    symbol: str
    entry_price: float
    stop_loss_price: float
    account_balance: float
    risk_percentage: float = 2.0  # Risk 2% of account per trade

class RiskMetrics(BaseModel):
    portfolio_value: float
    total_risk: float
    risk_percentage: float
    margin_used: float
    margin_available: float
    leverage_used: float
    max_leverage: float
    risk_score: str  # Low, Medium, High, Critical

@router.post("/position-sizing")
async def calculate_position_size(
    request: PositionSizingRequest,
    current_user: dict = Depends(get_current_active_user)
):
    """Calculate optimal position size based on risk management rules."""
    
    # Calculate risk amount
    risk_amount = request.account_balance * (request.risk_percentage / 100)
    
    # Calculate price risk
    price_risk = abs(request.entry_price - request.stop_loss_price)
    
    # Calculate position size
    position_size = risk_amount / price_risk
    
    # Apply position size limits
    max_position_value = request.account_balance * 0.1  # Max 10% of account per position
    max_position_size = max_position_value / request.entry_price
    
    final_position_size = min(position_size, max_position_size)
    
    return {
        "symbol": request.symbol,
        "calculated_size": final_position_size,
        "risk_amount": risk_amount,
        "price_risk": price_risk,
        "risk_percentage": request.risk_percentage,
        "max_position_size": max_position_size,
        "recommendation": "GOOD" if final_position_size > 0 else "INSUFFICIENT_RISK_BUDGET"
    }

@router.get("/limits")
async def get_risk_limits(
    current_user: dict = Depends(get_current_active_user)
):
    """Get current risk limits for the user."""
    # In a real implementation, this would be stored in the database
    limits = RiskLimits(
        max_position_size=10000.0,
        max_portfolio_risk=5.0,
        max_drawdown=10.0,
        max_leverage=20.0,
        max_correlation=0.7,
        stop_loss_percentage=2.0
    )
    
    return limits

@router.post("/limits")
async def update_risk_limits(
    limits: RiskLimits,
    current_user: dict = Depends(get_current_active_user)
):
    """Update risk limits for the user."""
    # In a real implementation, this would save to the database
    return {"message": "Risk limits updated successfully", "limits": limits}

@router.get("/metrics")
async def get_risk_metrics(
    api_key_id: str,
    current_user: dict = Depends(get_current_active_user)
):
    """Get current portfolio risk metrics."""
    # Mock data for now - replace with actual calculations
    metrics = RiskMetrics(
        portfolio_value=10000.0,
        total_risk=500.0,
        risk_percentage=5.0,
        margin_used=2000.0,
        margin_available=8000.0,
        leverage_used=2.0,
        max_leverage=20.0,
        risk_score="Medium"
    )
    
    return metrics

@router.post("/check")
async def check_risk_compliance(
    api_key_id: str,
    order_data: dict,
    current_user: dict = Depends(get_current_active_user)
):
    """Check if a proposed order complies with risk management rules."""
    
    # Mock risk checks
    checks = {
        "position_size_ok": True,
        "leverage_ok": True,
        "correlation_ok": True,
        "margin_ok": True,
        "drawdown_ok": True,
        "overall_compliant": True,
        "warnings": [],
        "errors": []
    }
    
    # Add some example warnings
    if order_data.get("quantity", 0) > 1000:
        checks["warnings"].append("Large position size detected")
    
    if order_data.get("leverage", 1) > 10:
        checks["warnings"].append("High leverage detected")
    
    return checks

@router.get("/heatmap")
async def get_risk_heatmap(
    api_key_id: str,
    current_user: dict = Depends(get_current_active_user)
):
    """Get portfolio risk heatmap showing concentration and correlation risks."""
    
    # Mock heatmap data
    heatmap = {
        "assets": ["BTC", "ETH", "ADA", "DOT", "LINK"],
        "weights": [0.4, 0.3, 0.15, 0.1, 0.05],
        "correlations": [
            [1.0, 0.8, 0.6, 0.7, 0.5],
            [0.8, 1.0, 0.5, 0.6, 0.4],
            [0.6, 0.5, 1.0, 0.4, 0.3],
            [0.7, 0.6, 0.4, 1.0, 0.6],
            [0.5, 0.4, 0.3, 0.6, 1.0]
        ],
        "risk_levels": ["Low", "Medium", "Low", "Medium", "Low"]
    }
    
    return heatmap


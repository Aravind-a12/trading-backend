from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from pydantic import BaseModel
from app.ingestion.order_manage import place_futures_order, cancel_futures_order
from app.ingestion.modify_order import modify_futures_order
from app.ingestion.user_data import get_open_orders

router = APIRouter()

# Pydantic model for order request
class PlaceOrderRequest(BaseModel):
    symbol: str
    side: str
    order_type: str
    quantity: float
    price: Optional[float] = None
    time_in_force: Optional[str] = "GTC"

placed_orders = []
cancelled_orders = []
updated_orders = []

# --------------------------------PLACE ORDER API---------------------
@router.post("/place-order")
async def place_order_api(order_data: PlaceOrderRequest):
   
    try:
        # Debug logging
        print(f"📝 Received order request: {order_data}")
        
        # Check if environment variables are set
        from app.ingestion.order_manage import API_KEY, API_SECRET, FUTURES_BASE_URL
        if not API_KEY or not API_SECRET:
            raise HTTPException(status_code=500, detail="API credentials not configured. Please check your .env file.")
        
        kwargs = {}
        if order_data.order_type.upper() == "LIMIT":
            if order_data.price is None:
                raise HTTPException(status_code=400, detail="Price is required for LIMIT orders.")
            kwargs["price"] = order_data.price
            kwargs["timeInForce"] = order_data.time_in_force.upper()
        
        print(f"🔧 Calling place_futures_order with: symbol={order_data.symbol}, side={order_data.side}, type={order_data.order_type}, quantity={order_data.quantity}, kwargs={kwargs}")
        
        response = place_futures_order(order_data.symbol, order_data.side, order_data.order_type, order_data.quantity, **kwargs)
        
        if response:
            print(f"✅ Order placed successfully: {response}")
            placed_orders.append(response)  # Save it for GET
            return response
        else:
            print("❌ place_futures_order returned None")
            raise HTTPException(status_code=500, detail="Failed to place order.")
    except Exception as e:
        print(f"❌ Error placing order: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/orders")
async def get_orders():
    """Get all open orders from Binance (not just in-memory orders)"""
    try:
        # Fetch actual open orders from Binance
        open_orders = get_open_orders()
        return open_orders
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/orders/{symbol}")
async def get_orders_by_symbol(symbol: str):
    """Get open orders for a specific symbol from Binance"""
    try:
        # Fetch actual open orders from Binance for the specific symbol
        open_orders = get_open_orders(symbol)
        return open_orders
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/local-orders")
async def get_local_orders():
    """Get orders placed through this API during current session (in-memory)"""
    try:
        return placed_orders
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -------------------------------MODIFY/UPDATE ORDER API-------------------------

@router.put("/modify-order")
async def modify_order_api(
    symbol: str = Query(..., description="Trading pair, e.g., BTCUSDT"),
    side: str = Query(..., description="Order side: BUY or SELL"),
    quantity: float = Query(..., description="Order quantity"),
    price: float = Query(..., description="Order price"),
    order_id: Optional[int] = Query(None, description="Order ID"),
    orig_client_order_id: Optional[str] = Query(None, description="Original client order ID")
):
    """
    Modify (amend) an existing futures order.
    Either order_id or orig_client_order_id must be provided.
    """
    if not order_id and not orig_client_order_id:
        raise HTTPException(status_code=400, detail="Either 'order_id' or 'orig_client_order_id' must be provided.")

    try:
        response = modify_futures_order(
            symbol=symbol,
            side=side,
            quantity=quantity,
            price=price,
            order_id=order_id,
            orig_client_order_id=orig_client_order_id
        )
        if response:
            updated_orders.append(response)
            return response
        else:
            raise HTTPException(status_code=500, detail="Failed to modify order.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/updated-orders")
async def get_updated_orders():
    """
    Get the list of all updated orders.
    """
    try:
        return updated_orders
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -------------------------------CANCEL ORDER API---------------------------
@router.post("/cancel-order")
async def cancel_order_api(
    symbol: str = Query(..., description="Trading pair, e.g., BTCUSDT"),
    order_id: Optional[int] = Query(None, description="Order ID to cancel"),
    orig_client_order_id: Optional[str] = Query(None, description="Client order ID to cancel")
):
    """
    Cancel an existing futures order by order ID or original client order ID.
    """
    try:
        if not order_id and not orig_client_order_id:
            raise HTTPException(status_code=400, detail="You must provide either order_id or orig_client_order_id.")

        response = cancel_futures_order(symbol, order_id=order_id, orig_client_order_id=orig_client_order_id)
        if response:
            cancelled_orders.append(response)  # Track cancelled orders
            return response
        else:
            raise HTTPException(status_code=500, detail="Failed to cancel order.")
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/cancelled-orders")
async def get_cancelled_orders():
    """
    Get the list of all cancelled orders.
    """
    try:
        return cancelled_orders
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

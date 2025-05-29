from fastapi import APIRouter,HTTPException,Query
from typing import Optional
from app.ingestion.order_manage import place_futures_order,cancel_futures_order
from app.ingestion.modify_order import modify_futures_order

router = APIRouter()

placed_orders=[]
cancelled_orders=[]
updated_orders=[]

# --------------------------------PLACE ORDER API---------------------
@router.post("/place-order")
async def place_order_api(
    symbol: str = Query(..., description="Trading pair, e.g., BTCUSDT"),
    side: str = Query(..., description="Order side: BUY or SELL"),
    order_type: str = Query(..., alias="type", description="Order type: LIMIT or MARKET"),
    quantity: float = Query(..., description="Order quantity"),
    price: Optional[float] = Query(None, description="Price for LIMIT orders"),
    time_in_force: Optional[str] = Query("GTC", description="Time in force for LIMIT orders")
):
    """
    Place a new futures order.
    """
    try:
        kwargs = {}
        if order_type.upper() == "LIMIT":
            if price is None:
                raise HTTPException(status_code=400, detail="Price is required for LIMIT orders.")
            kwargs["price"] = price
            kwargs["timeInForce"] = time_in_force.upper()
        
        response = place_futures_order(symbol, side, order_type, quantity, **kwargs)
        if response:
            placed_orders.append(response)  # Save it for GET
            return response
        else:
            raise HTTPException(status_code=500, detail="Failed to place order.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/orders")
async def get_orders():
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
    

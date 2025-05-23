import time
import hmac
import hashlib
import httpx
import os
from dotenv import load_dotenv
from urllib.parse import urlencode

# Load variables from .env
load_dotenv()

# Get environment variables
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
FUTURES_BASE_URL = os.getenv("FUTURES_BASE_URL")
ORDER_MANAGEMENT_ENDPOINT = os.getenv("ORDER_MANAGEMENT_ENDPOINT")

# ------------------- Collect user input BEFORE main ----------------------
symbol = input("Enter trading pair (e.g., BTCUSDT): ").strip()
side = input("Enter side (BUY or SELL): ").strip().upper()
type_ = input("Enter order type (LIMIT or MARKET): ").strip().upper()
quantity = float(input("Enter quantity: "))

# For LIMIT orders, we collect price and optionally timeInForce
kwargs = {}
if type_ == "LIMIT":
    price = float(input("Enter price: "))
    time_in_force = input("Enter timeInForce (default GTC): ").strip().upper() or "GTC"
    kwargs["price"] = price
    kwargs["timeInForce"] = time_in_force

# ------------------- Function definitions -------------------------------
def sign(params: dict, secret: str) -> str:
    """Generate HMAC SHA256 signature."""
    query_string = urlencode(params)
    signature = hmac.new(secret.encode(), query_string.encode(), hashlib.sha256).hexdigest()
    return f"{query_string}&signature={signature}"

def place_futures_order(symbol: str, side: str, type_: str, quantity: float, **kwargs):
    url = f"{FUTURES_BASE_URL}{ORDER_MANAGEMENT_ENDPOINT}"
    timestamp = int(time.time() * 1000)

    params = {
        "symbol": symbol.upper(),
        "side": side.upper(),
        "type": type_.upper(),
        "quantity": quantity,
        "timestamp": timestamp
    }

    if params["type"] == "LIMIT":
        if "timeInForce" not in kwargs:
            kwargs["timeInForce"] = "GTC"
        if "price" not in kwargs:
            raise ValueError("Missing required parameter 'price' for LIMIT order")

    params.update(kwargs)
    signed_query = sign(params, API_SECRET)
    full_url = f"{url}?{signed_query}"

    headers = {
        "X-MBX-APIKEY": API_KEY
    }

    with httpx.Client() as client:
        try:
            response = client.post(full_url, headers=headers)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            print(f"HTTP error: {e.response.status_code} - {e.response.text}")
        except Exception as e:
            print(f"Error placing order: {e}")

# ---------------------FUNCTION FOR MODIFYING ORDER---------------------
def modify_futures_order(symbol: str, order_id: int = None, orig_client_order_id: str = None,
                         quantity: float = None, price: float = None, time_in_force: str = "GTC"):
    """
    Modify an existing LIMIT order on Binance Futures.
    At least one of order_id or orig_client_order_id must be provided.
    """
    url = f"{FUTURES_BASE_URL}/fapi/v1/order"
    timestamp = int(time.time() * 1000)

    params = {
        "symbol": symbol.upper(),
        "timestamp": timestamp,
        "timeInForce": time_in_force
    }

    if order_id:
        params["orderId"] = order_id
    elif orig_client_order_id:
        params["origClientOrderId"] = orig_client_order_id
    else:
        raise ValueError("You must provide either order_id or orig_client_order_id to modify an order.")

    if price:
        params["price"] = price
    if quantity:
        params["quantity"] = quantity

    signed_query = sign(params, API_SECRET)
    full_url = f"{url}?{signed_query}"

    headers = {
        "X-MBX-APIKEY": API_KEY
    }

    with httpx.Client() as client:
        try:
            response = client.put(full_url, headers=headers)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            print(f"HTTP error: {e.response.status_code} - {e.response.text}")
        except Exception as e:
            print(f"Error modifying order: {e}")

# ------------------- Main execution block -------------------------------
if __name__ == "__main__":
    response = place_futures_order(symbol, side, type_, quantity, **kwargs)
    if response:
        print(response)
    else:
        print("No data returned.")
        
    order_id_to_modify = response["orderId"]
    new_price = float(input("Enter new price to modify the LIMIT order: "))
    new_quantity = float(input("Enter new quantity: "))

    modification_response = modify_futures_order(
    symbol=symbol,
    order_id=order_id_to_modify,
    price=new_price,
    quantity=new_quantity
  )

    if modification_response:
     print("Order modified:")
     print(modification_response)
    else:
     print("Failed to modify order.")
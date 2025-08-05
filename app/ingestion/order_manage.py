import time
import hmac
import hashlib
import httpx
import os
from dotenv import load_dotenv
from urllib.parse import urlencode

# Load environment variables from .env file
load_dotenv()

# Get environment variables
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
FUTURES_BASE_URL = os.getenv("FUTURES_BASE_URL")
ORDER_MANAGEMENT_ENDPOINT = os.getenv("ORDER_MANAGEMENT_ENDPOINT")

# ------------------- Utility: Sign requests -------------------------------
def sign(params: dict, secret: str) -> str:
    query_string = urlencode(params)
    signature = hmac.new(secret.encode(), query_string.encode(), hashlib.sha256).hexdigest()
    return f"{query_string}&signature={signature}"

# ------------------- Place order -----------------------------------------
def place_futures_order(
    symbol: str = None,
    side: str = None,
    order_type: str = None,
    quantity: float = None,
    price: float = None,
    timeInForce: str = "GTC"
):
    # Manual input fallback (for CLI usage)
    if symbol is None:
        symbol = input("Enter trading pair (e.g., BTCUSDT): ").strip()
    if side is None:
        side = input("Enter side (BUY or SELL): ").strip().upper()
    if order_type is None:
        order_type = input("Enter order type (LIMIT or MARKET): ").strip().upper()
    if quantity is None:
        quantity = float(input("Enter quantity: "))

    if order_type == "LIMIT":
        if price is None:
            price = float(input("Enter price: "))
        if not timeInForce:
            timeInForce = input("Enter timeInForce (default GTC): ").strip().upper() or "GTC"

    url = f"{FUTURES_BASE_URL}{ORDER_MANAGEMENT_ENDPOINT}"
    timestamp = int(time.time() * 1000)

    params = {
        "symbol": symbol.upper(),
        "side": side.upper(),
        "type": order_type.upper(),
        "quantity": quantity,
        "timestamp": timestamp,
        "recvWindow": 10000  # 10 seconds to account for clock skew
    }

    if order_type.upper() == "LIMIT":
        params["price"] = price
        params["timeInForce"] = timeInForce.upper()

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
            raise Exception(f"HTTP error: {e.response.status_code} - {e.response.text}")
        except Exception as e:
            raise Exception(f"Error placing order: {e}")

# ------------------- Cancel order ----------------------------------------
def cancel_futures_order(
    symbol: str = None,
    order_id: int = None,
    orig_client_order_id: str = None
):
    if symbol is None:
        symbol = input("Enter trading pair (e.g., BTCUSDT): ").strip()

    if order_id is None and orig_client_order_id is None:
        choice = input("Cancel by order ID (1) or origClientOrderId (2)? Enter 1 or 2: ").strip()
        if choice == "1":
            order_id = int(input("Enter order ID: "))
        elif choice == "2":
            orig_client_order_id = input("Enter origClientOrderId: ")
        else:
            print("Invalid choice. Exiting.")
            return

    url = f"{FUTURES_BASE_URL}{ORDER_MANAGEMENT_ENDPOINT}"
    timestamp = int(time.time() * 1000)

    params = {
        "symbol": symbol.upper(),
        "timestamp": timestamp,
        "recvWindow": 10000  # Add recvWindow here too
    }

    if order_id:
        params["orderId"] = order_id
    elif orig_client_order_id:
        params["origClientOrderId"] = orig_client_order_id
    else:
        raise ValueError("Either order_id or orig_client_order_id must be provided")

    signed_query = sign(params, API_SECRET)
    full_url = f"{url}?{signed_query}"

    headers = {
        "X-MBX-APIKEY": API_KEY
    }

    with httpx.Client() as client:
        try:
            response = client.delete(full_url, headers=headers)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            raise Exception(f"HTTP error: {e.response.status_code} - {e.response.text}")
        except Exception as e:
            raise Exception(f"Error canceling order: {e}")

# ------------------- Main CLI logic --------------------------------------
if __name__ == "__main__":
    action = input("Do you want to place or cancel an order? (place/cancel): ").strip().lower()
    if action == "place":
        result = place_futures_order()
        if result:
            print("✅ Order placed:")
            print(result)
        else:
            print("❌ Failed to place order.")
    elif action == "cancel":
        result = cancel_futures_order()
        if result:
            print("✅ Order cancelled:")
            print(result)
        else:
            print("❌ Failed to cancel order.")
    else:
        print("Invalid action. Please choose 'place' or 'cancel'.")

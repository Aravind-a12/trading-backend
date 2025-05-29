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

def sign(params: dict, secret: str) -> str:
    query_string = urlencode(params)
    signature = hmac.new(secret.encode(), query_string.encode(), hashlib.sha256).hexdigest()
    return f"{query_string}&signature={signature}"
  
# ------------------- Modify (Amend) order -----------------------------------------
def modify_futures_order(
    symbol: str,
    side:str,
    quantity: float,
    price: float,
    order_id: int = None,
    orig_client_order_id: str = None
):
    url = f"{FUTURES_BASE_URL}{ORDER_MANAGEMENT_ENDPOINT}"
    timestamp = int(time.time() * 1000)

    if not order_id and not orig_client_order_id:
        raise ValueError("Either 'order_id' or 'orig_client_order_id' must be provided.")

    params = {
        "symbol": symbol.upper(),
        "side":side,
        "quantity": quantity,
        "price": price,
        "timestamp": timestamp
    }

    if order_id:
        params["orderId"] = order_id
    else:
        params["origClientOrderId"] = orig_client_order_id

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

def main():
    symbol = input("Enter symbol (e.g., BTCUSDT): ").strip()
    side=input("Enter side BUY or SELL : ")
    quantity = float(input("Enter quantity (e.g., 0.01): "))
    price = float(input("Enter price (e.g., 28500.00): "))

    id_choice = input("Use order ID (1) or origClientOrderId (2)? ")
    order_id = None
    orig_client_order_id = None

    if id_choice == "1":
        order_id = int(input("Enter order ID: "))
    elif id_choice == "2":
        orig_client_order_id = input("Enter original client order ID: ").strip()

    result = modify_futures_order(
        symbol=symbol,
        quantity=quantity,
        side=side,
        price=price,
        order_id=order_id,
        orig_client_order_id=orig_client_order_id
    )

    print("Order modify result:")
    print(result)

if __name__ == "__main__":
    main()

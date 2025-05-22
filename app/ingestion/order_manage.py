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

# ------------------- Main execution block -------------------------------
if __name__ == "__main__":
    response = place_futures_order(symbol, side, type_, quantity, **kwargs)
    if response:
        print(response)
    else:
        print("No data returned.")
        
# this is python

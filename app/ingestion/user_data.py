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

ACC_BALANCE_ENDPOINT = os.getenv("ACC_BALANCE_ENDPOINT")
ALL_ORDERS_ENDPOINT = os.getenv("ALL_ORDERS_ENDPOINT")
OPEN_ORDERS_ENDPOINT = os.getenv("OPEN_ORDERS_ENDPOINT")
POSITION_RISK_ENDPOINT = os.getenv("POSITION_RISK_ENDPOINT")
TRADE_HISTORY_ENDPOINT = os.getenv("TRADE_HISTORY_ENDPOINT")

def sign(params: dict, secret: str) -> str:
    """Generate HMAC SHA256 signature."""
    query_string = urlencode(params)
    signature = hmac.new(secret.encode(), query_string.encode(), hashlib.sha256).hexdigest()
    return f"{query_string}&signature={signature}"
  
# --------------------FUNCTION FOR ACCOUNT BALANCE--------------- 
def get_futures_account_balance_v3():
    url = f"{FUTURES_BASE_URL}{ACC_BALANCE_ENDPOINT}"
    timestamp = int(time.time() * 1000)
    params = {
        "timestamp": timestamp
    }
    signed_query = sign(params, API_SECRET)
    full_url = f"{url}?{signed_query}"

    headers = {
        "X-MBX-APIKEY": API_KEY
    }

    with httpx.Client() as client:
        try:
            response = client.get(full_url, headers=headers)
            response.raise_for_status()
            data = response.json()
            return data
        except httpx.HTTPStatusError as e:
            print(f"HTTP error: {e.response.status_code} - {e.response.text}")
        except Exception as e:
            print(f"Error fetching balance: {e}")
            
# --------------------FUNCTION FOR GETTING ALL THE ORDERS---------------           
def get_all_the_orders(symbol: str):
    """Fetch all orders for a given symbol."""
    url = f"{FUTURES_BASE_URL}{ALL_ORDERS_ENDPOINT}"
    timestamp = int(time.time() * 1000)

    params = {
        'symbol': symbol.upper(),
        'timestamp': timestamp,
        'limit': 50
    }

    signed_query = sign(params, API_SECRET)
    full_url = f"{url}?{signed_query}"
    headers = {'X-MBX-APIKEY': API_KEY}

    with httpx.Client() as client:
        try:
            response = client.get(full_url, headers=headers)
            response.raise_for_status()
            orders = response.json()
            return orders

        except httpx.HTTPStatusError as e:
            print(f"HTTP error: {e.response.status_code} - {e.response.text}")
        except Exception as e:
            print(f"Error fetching orders: {e}")

# --------------------FUNCTION FOR GETTING ALL THE OPEN ORDERS---------------           
def get_open_orders(symbol: str = None):
    """
    Fetch open futures orders. Optionally filter by symbol.
    """
    url = f"{FUTURES_BASE_URL}{OPEN_ORDERS_ENDPOINT}"
    timestamp = int(time.time() * 1000)

    params = {
        'timestamp': timestamp,
    }

    if symbol:
        params['symbol'] = symbol.upper()

    signed_query = sign(params, API_SECRET)
    full_url = f"{url}?{signed_query}"

    headers = {
        'X-MBX-APIKEY': API_KEY
    }

    with httpx.Client() as client:
        try:
            response = client.get(full_url, headers=headers)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            print(f"HTTP error: {e.response.status_code} - {e.response.text}")
        except Exception as e:
            print(f"Error: {e}")
            
# --------------------FUNCTION FOR GETTING POSITION INFO---------------           
def get_position_risk(symbol: str = None):
    """
    Fetch futures position risk. Optionally filter by symbol.
    """
    url = f"{FUTURES_BASE_URL}{POSITION_RISK_ENDPOINT}"
    timestamp = int(time.time() * 1000)
    params = {'timestamp': timestamp}

    if symbol:
        params['symbol'] = symbol.upper()

    signed_query = sign(params, API_SECRET)
    full_url = f"{url}?{signed_query}"
    headers = {'X-MBX-APIKEY': API_KEY}

    with httpx.Client() as client:
        try:
            response = client.get(full_url, headers=headers)
            response.raise_for_status()
            positions = response.json()

            if symbol:
                positions = [pos for pos in positions if pos["symbol"] == symbol.upper()]

            return positions
        except httpx.HTTPStatusError as e:
            print(f"HTTP error: {e.response.status_code} - {e.response.text}")
        except Exception as e:
            print(f"Error: {e}")

# --------------------FUNCTION FOR GETTING TRADE HISTORY---------------           
def get_trade_history(symbol: str):
    """
    Fetch trade history for a specific symbol.
    """
    url = f"{FUTURES_BASE_URL}{TRADE_HISTORY_ENDPOINT}"
    timestamp = int(time.time() * 1000)

    params = {
        'symbol': symbol.upper(),
        'timestamp': timestamp,
        'limit': 50
    }

    signed_query = sign(params, API_SECRET)
    full_url = f"{url}?{signed_query}"
    headers = {'X-MBX-APIKEY': API_KEY}

    with httpx.Client() as client:
        try:
            response = client.get(full_url, headers=headers)
            response.raise_for_status()
            trades = response.json()
            return trades
        except httpx.HTTPStatusError as e:
            print(f"HTTP error: {e.response.status_code} - {e.response.text}")
        except Exception as e:
            print(f"Error fetching trade history: {e}")

if __name__ == "__main__":
  
    # -------------------CALL ACCOUNT BALANCE----------------------
    acc_balance = get_futures_account_balance_v3()
    if acc_balance:
        print(acc_balance)
    else:
        print("No data returned.")
        
    # -------------------CALL ALL ORDERS----------------------
    orders = get_all_the_orders("BTCUSDT")
    if orders:
        print(orders)
    else:
        print("No data returned.")   
        
    # # -------------------CALL OPEN ORDERS----------------------
    open_orders = get_open_orders("BTCUSDT")
    if open_orders:
        print(open_orders)
    else:
        print("No data returned.")
    
    # # -------------------CALL POSITION RISK----------------------
    positions = get_position_risk()
    if positions:
        print(positions)
    else:
        print("No position data returned.")

    # # -------------------CALL TRADE HISTORY------------------------
    trade_history=get_trade_history("BTCUSDT")
    if trade_history:
      print(trade_history) 
    else:
      print("No Trade history found")  
   

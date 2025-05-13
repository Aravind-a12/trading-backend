
import time
import hmac
import hashlib
import httpx
import json
from typing import Dict, Any

API_KEY = '5c7c3366d0f9a941e56e93b07a1cf45476dca90478f1e8302397405bb782f5eb'
API_SECRET = '1fde4e8690060be4a6af24ced9c2eab36add4d36092562f2ed2bc222bf41e709'

BASE_URL = 'https://testnet.binancefuture.com'


def log(msg: str):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


def sign(params: Dict[str, Any], secret: str) -> str:
    query = '&'.join(f"{k}={v}" for k, v in sorted(params.items()))
    signature = hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()
    return f"{query}&signature={signature}"


def get_open_positions():
    endpoint = '/fapi/v2/positionRisk'
    url = f"{BASE_URL}{endpoint}"
    timestamp = int(time.time() * 1000)
    params = {'timestamp': timestamp}
    signed_query = sign(params, API_SECRET)

    headers = {'X-MBX-APIKEY': API_KEY}
    full_url = f"{url}?{signed_query}"

    with httpx.Client() as client:
        try:
            response = client.get(full_url, headers=headers)
            response.raise_for_status()
            data = response.json()
        except httpx.HTTPStatusError as e:
            log(f"HTTP error while fetching positions: {e}")
            log(f"Response: {response.text}")
            return
        except Exception as e:
            log(f"Unexpected error while fetching positions: {e}")
            return

    log("Raw fetched position data:")
    print(json.dumps(data, indent=2))

    if not isinstance(data, list):
        log("Unexpected response format. Expected a list.")
        return

    for pos in data:
        try:
            amt = float(pos.get('positionAmt', 0))
            if amt != 0:
                info = {
                    'symbol': pos.get('symbol'),
                    'positionAmt': pos.get('positionAmt'),
                    'entryPrice': pos.get('entryPrice'),
                    'unrealizedPnl': pos.get('unrealizedProfit'),
                    'leverage': pos.get('leverage'),
                    'side': 'LONG' if amt > 0 else 'SHORT'
                }
                log(f"Open Position: {info}")
                close_position(pos['symbol'], amt)
        except Exception as e:
            log(f"Error processing position: {e}")
            log(f"Position data: {pos}")


def close_position(symbol: str, position_amt: float):
    endpoint = '/fapi/v1/order'
    url = f"{BASE_URL}{endpoint}"
    timestamp = int(time.time() * 1000)
    side = 'SELL' if position_amt > 0 else 'BUY'
    quantity = abs(position_amt)

    params = {
        'symbol': symbol,
        'side': side,
        'type': 'MARKET',
        'quantity': quantity,
        'reduceOnly': 'true',
        'timestamp': timestamp
    }

    signed_query = sign(params, API_SECRET)
    full_url = f"{url}?{signed_query}"
    headers = {'X-MBX-APIKEY': API_KEY}

    with httpx.Client() as client:
        try:
            response = client.post(full_url, headers=headers)
            response.raise_for_status()
            log(f"Closed position for {symbol}: {response.json()}")
        except httpx.HTTPStatusError as e:
            log(f"HTTP error while closing position for {symbol}: {e}")
            log(f"Response: {response.text}")
        except Exception as e:
            log(f"Unexpected error while closing position for {symbol}: {e}")

def get_futures_account_balance():
    url = f"{BASE_URL}/fapi/v2/balance"
    timestamp = int(time.time() * 1000)
    params = {'timestamp': timestamp}
    signed_query = sign(params, API_SECRET)

    headers = {'X-MBX-APIKEY': API_KEY}
    full_url = f"{url}?{signed_query}"

    with httpx.Client() as client:
        try:
            response = client.get(full_url, headers=headers)
            response.raise_for_status()
            data = response.json()
            print("Futures Account Balances:")
            for asset in data:
                print(f"{asset['asset']}: {asset['balance']} (Available: {asset['availableBalance']})")
            return data
        except Exception as e:
            print(f"Error fetching account balance: {e}")
            
def get_trade_history(symbol: str):
    url = f"{BASE_URL}/fapi/v1/userTrades"
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

            print("Recent Trades:")
            if not trades:
                print("No trades found for this symbol.")
            else:
                trade_list = []
                for trade in trades:
                    trade_info = {
                        "Symbol": trade["symbol"],
                        "Price": trade["price"],
                        "Qty": trade["qty"],
                        "Side": "BUY" if trade.get("isBuyer") else "SELL",
                        "Time": trade["time"]
                    }
                    trade_list.append(trade_info)
                    print(trade_info)
                return trade_list

        except httpx.HTTPStatusError as e:
            print(f"HTTP error: {e.response.status_code} - {e.response.text}")
        except Exception as e:
            print(f"Error: {e}")
            
def get_all_the_orders(symbol: str):
    url = f"{BASE_URL}/fapi/v1/allOrders"
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

            print("Get all the orders:")
            if not orders:
                print("No orders found for this symbol.")
            else:
                for order in orders:
                    print({
                        "Order ID": order["orderId"],
                        "Symbol": order["symbol"],
                        "Side": order["side"],
                        "Type": order["type"],
                        "Status": order["status"],
                        "Price": order["price"],
                        "Qty": order["origQty"],
                        "Executed": order["executedQty"],
                        "Time": order["time"]
                    })
                return orders

        except httpx.HTTPStatusError as e:
            print(f"HTTP error: {e.response.status_code} - {e.response.text}")
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
  get_open_positions()
  get_futures_account_balance()
  get_trade_history("ETHUSDT")
  get_all_the_orders("ETHUSDT")
  

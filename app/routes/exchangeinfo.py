import requests
import time
import hmac
import hashlib
import urllib.parse

# Your Binance API credentials
API_KEY = "5c7c3366d0f9a941e56e93b07a1cf45476dca90478f1e8302397405bb782f5eb"
SECRET_KEY = "1fde4e8690060be4a6af24ced9c2eab36add4d36092562f2ed2bc222bf41e709"

def fetch_data(url, headers=None, params=None, signed=False):
    try:
        if signed:
            query_string = urllib.parse.urlencode(params)
            signature = hmac.new(
                SECRET_KEY.encode('utf-8'),
                query_string.encode('utf-8'),
                hashlib.sha256
            ).hexdigest()
            params['signature'] = signature

        response = requests.get(url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        print(f"❌ HTTP error occurred: {http_err} - URL: {url}")
    except requests.exceptions.ConnectionError as conn_err:
        print(f"❌ Connection error occurred: {conn_err} - URL: {url}")
    except requests.exceptions.Timeout as timeout_err:
        print(f"❌ Timeout error occurred: {timeout_err} - URL: {url}")
    except requests.exceptions.RequestException as req_err:
        print(f"❌ General error occurred: {req_err} - URL: {url}")
    except ValueError as json_err:
        print(f"❌ JSON decode error: {json_err} - URL: {url}")
        print(f"🔍 Response content was: {response.text[:200]}")
    return None

def get_spot_exchange_info():
    url = "https://api.binance.com/api/v3/exchangeInfo"
    return fetch_data(url)

def get_futures_exchange_info():
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    return fetch_data(url)

def get_margin_account_info():
    url = "https://api.binance.com/sapi/v1/margin/account"
    params = {
        "timestamp": int(time.time() * 1000)
    }
    headers = {
        "X-MBX-APIKEY": API_KEY
    }
    return fetch_data(url, headers=headers, params=params, signed=True)

# Example usage
if __name__ == "__main__":
    print("🔸 Spot Exchange Info Sample:")
    spot_info = get_spot_exchange_info()
    if spot_info and "symbols" in spot_info:
        print(spot_info["symbols"][:2])
    else:
        print("⚠️ Failed to retrieve Spot exchange info.")

    print("\n🔸 Futures Exchange Info Sample:")
    futures_info = get_futures_exchange_info()
    if futures_info and "symbols" in futures_info:
        print(futures_info["symbols"][:2])
    else:
        print("⚠️ Failed to retrieve Futures exchange info.")

    print("\n🔸 Margin Account Info Sample:")
    margin_info = get_margin_account_info()
    if margin_info and "userAssets" in margin_info:
        print(margin_info["userAssets"][:2])
    else:
        print("⚠️ Failed to retrieve Margin account info.")

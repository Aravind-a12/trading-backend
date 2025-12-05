"""
Comprehensive data fetcher for all trading information from exchange APIs.
This fetches all available data from user's API keys.
"""

import httpx
import hmac
import hashlib
import time
from typing import Dict, Any, List, Optional
from urllib.parse import urlencode
from app.models.api_key import ExchangeType
from app.utils.encryption import decrypt_data

class TradingDataFetcher:
    """Fetches comprehensive trading data from exchanges."""
    
    def __init__(self, api_key: str, secret_key: str, passphrase: str = None):
        self.api_key = api_key
        self.secret_key = secret_key
        self.passphrase = passphrase
    
    def sign_request(self, params: dict, secret: str) -> str:
        """Generate HMAC SHA256 signature for Binance-style exchanges."""
        query_string = urlencode(params)
        signature = hmac.new(secret.encode(), query_string.encode(), hashlib.sha256).hexdigest()
        return f"{query_string}&signature={signature}"
    
    async def fetch_all_data(self, exchange: ExchangeType) -> Dict[str, Any]:
        """Fetch all available trading data from the exchange."""
        if exchange in [ExchangeType.BINANCE, ExchangeType.BINANCE_FUTURES]:
            return await self._fetch_binance_data()
        elif exchange == ExchangeType.COINBASE_PRO:
            return await self._fetch_coinbase_data()
        else:
            return {"error": f"Exchange {exchange} not supported yet"}
    
    async def _fetch_binance_data(self) -> Dict[str, Any]:
        """Fetch comprehensive data from Binance."""
        base_url = "https://fapi.binance.com"  # Binance Futures
        all_data = {
            "exchange": "binance_futures",
            "timestamp": int(time.time() * 1000),
            "account_info": {},
            "balances": [],
            "open_orders": [],
            "order_history": [],
            "trade_history": [],
            "positions": [],
            "funding_history": [],
            "income_history": [],
            "exchange_info": {},
            "ticker_24hr": [],
            "open_interest": [],
            "long_short_ratio": [],
            "funding_rate": []
        }
        
        async with httpx.AsyncClient() as client:
            headers = {"X-MBX-APIKEY": self.api_key}
            
            # 1. Account Information
            try:
                timestamp = int(time.time() * 1000)
                params = {"timestamp": timestamp}
                signed_query = self.sign_request(params, self.secret_key)
                url = f"{base_url}/fapi/v2/account?{signed_query}"
                
                response = await client.get(url, headers=headers)
                if response.status_code == 200:
                    all_data["account_info"] = response.json()
            except Exception as e:
                all_data["account_info"] = {"error": str(e)}
            
            # 2. All Balances
            try:
                timestamp = int(time.time() * 1000)
                params = {"timestamp": timestamp}
                signed_query = self.sign_request(params, self.secret_key)
                url = f"{base_url}/fapi/v2/balance?{signed_query}"
                
                response = await client.get(url, headers=headers)
                if response.status_code == 200:
                    balances = response.json()
                    all_data["balances"] = [
                        {
                            "asset": b.get("asset"),
                            "balance": b.get("balance"),
                            "cross_wallet_balance": b.get("crossWalletBalance"),
                            "cross_un_pnl": b.get("crossUnPnl"),
                            "available_balance": b.get("availableBalance"),
                            "max_withdraw_amount": b.get("maxWithdrawAmount"),
                            "margin_available": b.get("marginAvailable"),
                            "update_time": b.get("updateTime")
                        }
                        for b in balances if float(b.get("balance", 0)) > 0
                    ]
            except Exception as e:
                all_data["balances"] = [{"error": str(e)}]
            
            # 3. Open Orders
            try:
                timestamp = int(time.time() * 1000)
                params = {"timestamp": timestamp}
                signed_query = self.sign_request(params, self.secret_key)
                url = f"{base_url}/fapi/v1/openOrders?{signed_query}"
                
                response = await client.get(url, headers=headers)
                if response.status_code == 200:
                    all_data["open_orders"] = response.json()
            except Exception as e:
                all_data["open_orders"] = [{"error": str(e)}]
            
            # 4. Order History (last 100 orders)
            try:
                timestamp = int(time.time() * 1000)
                params = {"timestamp": timestamp, "limit": 100}
                signed_query = self.sign_request(params, self.secret_key)
                url = f"{base_url}/fapi/v1/allOrders?{signed_query}"
                
                response = await client.get(url, headers=headers)
                if response.status_code == 200:
                    all_data["order_history"] = response.json()
            except Exception as e:
                all_data["order_history"] = [{"error": str(e)}]
            
            # 5. Trade History (last 100 trades)
            try:
                timestamp = int(time.time() * 1000)
                params = {"timestamp": timestamp, "limit": 100}
                signed_query = self.sign_request(params, self.secret_key)
                url = f"{base_url}/fapi/v1/userTrades?{signed_query}"
                
                response = await client.get(url, headers=headers)
                if response.status_code == 200:
                    all_data["trade_history"] = response.json()
            except Exception as e:
                all_data["trade_history"] = [{"error": str(e)}]
            
            # 6. Positions
            try:
                timestamp = int(time.time() * 1000)
                params = {"timestamp": timestamp}
                signed_query = self.sign_request(params, self.secret_key)
                url = f"{base_url}/fapi/v2/positionRisk?{signed_query}"
                
                response = await client.get(url, headers=headers)
                if response.status_code == 200:
                    positions = response.json()
                    all_data["positions"] = [
                        {
                            "symbol": p.get("symbol"),
                            "position_amt": p.get("positionAmt"),
                            "entry_price": p.get("entryPrice"),
                            "mark_price": p.get("markPrice"),
                            "unrealized_pnl": p.get("unRealizedProfit"),
                            "liquidation_price": p.get("liquidationPrice"),
                            "leverage": p.get("leverage"),
                            "max_notional": p.get("maxNotionalValue"),
                            "margin_type": p.get("marginType"),
                            "isolated_margin": p.get("isolatedMargin"),
                            "is_auto_add_margin": p.get("isAutoAddMargin"),
                            "position_side": p.get("positionSide"),
                            "notional": p.get("notional"),
                            "isolated_wallet": p.get("isolatedWallet"),
                            "update_time": p.get("updateTime")
                        }
                        for p in positions if float(p.get("positionAmt", 0)) != 0
                    ]
            except Exception as e:
                all_data["positions"] = [{"error": str(e)}]
            
            # 7. Funding History
            try:
                timestamp = int(time.time() * 1000)
                params = {"timestamp": timestamp, "limit": 100}
                signed_query = self.sign_request(params, self.secret_key)
                url = f"{base_url}/fapi/v1/fundingRate?{signed_query}"
                
                response = await client.get(url, headers=headers)
                if response.status_code == 200:
                    all_data["funding_history"] = response.json()
            except Exception as e:
                all_data["funding_history"] = [{"error": str(e)}]
            
            # 8. Income History
            try:
                timestamp = int(time.time() * 1000)
                params = {"timestamp": timestamp, "limit": 100}
                signed_query = self.sign_request(params, self.secret_key)
                url = f"{base_url}/fapi/v1/income?{signed_query}"
                
                response = await client.get(url, headers=headers)
                if response.status_code == 200:
                    all_data["income_history"] = response.json()
            except Exception as e:
                all_data["income_history"] = [{"error": str(e)}]
            
            # 9. Exchange Information
            try:
                url = f"{base_url}/fapi/v1/exchangeInfo"
                response = await client.get(url)
                if response.status_code == 200:
                    all_data["exchange_info"] = response.json()
            except Exception as e:
                all_data["exchange_info"] = {"error": str(e)}
            
            # 10. 24hr Ticker Statistics
            try:
                url = f"{base_url}/fapi/v1/ticker/24hr"
                response = await client.get(url)
                if response.status_code == 200:
                    all_data["ticker_24hr"] = response.json()
            except Exception as e:
                all_data["ticker_24hr"] = [{"error": str(e)}]
            
            # 11. Open Interest
            try:
                url = f"{base_url}/fapi/v1/openInterest"
                response = await client.get(url)
                if response.status_code == 200:
                    all_data["open_interest"] = response.json()
            except Exception as e:
                all_data["open_interest"] = [{"error": str(e)}]
            
            # 12. Long/Short Ratio
            try:
                url = f"{base_url}/fapi/v1/globalLongShortAccountRatio"
                response = await client.get(url)
                if response.status_code == 200:
                    all_data["long_short_ratio"] = response.json()
            except Exception as e:
                all_data["long_short_ratio"] = [{"error": str(e)}]
            
            # 13. Funding Rate
            try:
                url = f"{base_url}/fapi/v1/premiumIndex"
                response = await client.get(url)
                if response.status_code == 200:
                    all_data["funding_rate"] = response.json()
            except Exception as e:
                all_data["funding_rate"] = [{"error": str(e)}]
        
        return all_data
    
    async def _fetch_coinbase_data(self) -> Dict[str, Any]:
        """Fetch comprehensive data from Coinbase Pro."""
        base_url = "https://api.pro.coinbase.com"
        all_data = {
            "exchange": "coinbase_pro",
            "timestamp": int(time.time()),
            "accounts": [],
            "orders": [],
            "fills": [],
            "products": [],
            "currencies": [],
            "time": {},
            "fees": {}
        }
        
        async with httpx.AsyncClient() as client:
            # 1. Accounts
            try:
                path = "/accounts"
                timestamp = str(int(time.time()))
                headers = self._sign_coinbase_request("GET", path, "", timestamp)
                headers["Content-Type"] = "application/json"
                
                response = await client.get(f"{base_url}{path}", headers=headers)
                if response.status_code == 200:
                    all_data["accounts"] = response.json()
            except Exception as e:
                all_data["accounts"] = [{"error": str(e)}]
            
            # 2. Orders
            try:
                path = "/orders"
                timestamp = str(int(time.time()))
                headers = self._sign_coinbase_request("GET", path, "", timestamp)
                headers["Content-Type"] = "application/json"
                
                response = await client.get(f"{base_url}{path}", headers=headers)
                if response.status_code == 200:
                    all_data["orders"] = response.json()
            except Exception as e:
                all_data["orders"] = [{"error": str(e)}]
            
            # 3. Fills (Trades)
            try:
                path = "/fills"
                timestamp = str(int(time.time()))
                headers = self._sign_coinbase_request("GET", path, "", timestamp)
                headers["Content-Type"] = "application/json"
                
                response = await client.get(f"{base_url}{path}", headers=headers)
                if response.status_code == 200:
                    all_data["fills"] = response.json()
            except Exception as e:
                all_data["fills"] = [{"error": str(e)}]
            
            # 4. Products
            try:
                response = await client.get(f"{base_url}/products")
                if response.status_code == 200:
                    all_data["products"] = response.json()
            except Exception as e:
                all_data["products"] = [{"error": str(e)}]
            
            # 5. Currencies
            try:
                response = await client.get(f"{base_url}/currencies")
                if response.status_code == 200:
                    all_data["currencies"] = response.json()
            except Exception as e:
                all_data["currencies"] = [{"error": str(e)}]
            
            # 6. Time
            try:
                response = await client.get(f"{base_url}/time")
                if response.status_code == 200:
                    all_data["time"] = response.json()
            except Exception as e:
                all_data["time"] = {"error": str(e)}
            
            # 7. Fees
            try:
                path = "/fees"
                timestamp = str(int(time.time()))
                headers = self._sign_coinbase_request("GET", path, "", timestamp)
                headers["Content-Type"] = "application/json"
                
                response = await client.get(f"{base_url}{path}", headers=headers)
                if response.status_code == 200:
                    all_data["fees"] = response.json()
            except Exception as e:
                all_data["fees"] = {"error": str(e)}
        
        return all_data
    
    def _sign_coinbase_request(self, method: str, path: str, body: str = "", timestamp: str = None) -> Dict[str, str]:
        """Generate Coinbase Pro signature."""
        if timestamp is None:
            timestamp = str(int(time.time()))
        
        message = timestamp + method + path + body
        signature = hmac.new(
            self.secret_key.encode('utf-8'),
            message.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        
        return {
            "CB-ACCESS-KEY": self.api_key,
            "CB-ACCESS-SIGN": signature,
            "CB-ACCESS-TIMESTAMP": timestamp,
            "CB-ACCESS-PASSPHRASE": self.passphrase
        }

async def fetch_all_trading_data(api_key_id: str, user_id: str) -> Dict[str, Any]:
    """Fetch all trading data for a specific API key."""
    from app.utils.memory_storage import get_storage_client
    import json
    
    storage_client = await get_storage_client()
    
    # Get API key data
    api_key_data = await storage_client.get(f"api_key:{api_key_id}")
    if not api_key_data:
        return {"error": "API key not found"}
    
    # Check if user owns this API key
    if not await storage_client.sismember(f"user_api_keys:{user_id}", api_key_id):
        return {"error": "Access denied"}
    
    api_key = json.loads(api_key_data)
    
    # Decrypt API key data
    decrypted_api_key = decrypt_data(api_key["api_key_encrypted"])
    decrypted_secret = decrypt_data(api_key["secret_key_encrypted"])
    decrypted_passphrase = None
    if api_key.get("passphrase_encrypted"):
        decrypted_passphrase = decrypt_data(api_key["passphrase_encrypted"])
    
    # Create data fetcher
    fetcher = TradingDataFetcher(decrypted_api_key, decrypted_secret, decrypted_passphrase)
    
    # Fetch all data
    all_data = await fetcher.fetch_all_data(api_key["exchange"])
    
    return all_data

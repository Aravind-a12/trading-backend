import httpx
import hmac
import hashlib
import time
from typing import Dict, Any, Optional
from urllib.parse import urlencode
from app.models.api_key import ExchangeType
from app.utils.encryption import decrypt_data

class ExchangeIntegration:
    """Base class for exchange integrations."""
    
    def __init__(self, api_key: str, secret_key: str, passphrase: str = None):
        self.api_key = api_key
        self.secret_key = secret_key
        self.passphrase = passphrase
    
    def sign_request(self, params: dict, secret: str) -> str:
        """Generate HMAC SHA256 signature for Binance-style exchanges."""
        query_string = urlencode(params)
        signature = hmac.new(secret.encode(), query_string.encode(), hashlib.sha256).hexdigest()
        return f"{query_string}&signature={signature}"
    
    async def test_connection(self) -> Dict[str, Any]:
        """Test API key connection. Override in subclasses."""
        raise NotImplementedError

class BinanceIntegration(ExchangeIntegration):
    """Binance exchange integration."""
    
    def __init__(self, api_key: str, secret_key: str, passphrase: str = None):
        super().__init__(api_key, secret_key, passphrase)
        self.base_url = "https://fapi.binance.com"  # Binance Futures
    
    async def test_connection(self) -> Dict[str, Any]:
        """Test Binance API key connection with real trading data."""
        try:
            timestamp = int(time.time() * 1000)
            params = {"timestamp": timestamp}
            
            signed_query = self.sign_request(params, self.secret_key)
            url = f"{self.base_url}/fapi/v2/account?{signed_query}"
            
            headers = {"X-MBX-APIKEY": self.api_key}
            
            async with httpx.AsyncClient() as client:
                response = await client.get(url, headers=headers)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Get additional trading data for validation
                    trading_data = await self._get_trading_data(client, headers)
                    
                    return {
                        "success": True,
                        "message": "Binance API key connection successful",
                        "exchange_info": {
                            "account_type": data.get("accountType", "Unknown"),
                            "can_trade": data.get("canTrade", False),
                            "can_withdraw": data.get("canWithdraw", False),
                            "total_wallet_balance": data.get("totalWalletBalance", "0"),
                            "total_margin_balance": data.get("totalMarginBalance", "0"),
                            "open_orders_count": trading_data.get("open_orders_count", 0),
                            "recent_trades_count": trading_data.get("recent_trades_count", 0),
                            "positions_count": trading_data.get("positions_count", 0),
                            "sample_balances": trading_data.get("sample_balances", []),
                            "last_activity": trading_data.get("last_activity", "No recent activity")
                        }
                    }
                else:
                    return {
                        "success": False,
                        "message": f"Binance API error: {response.status_code}",
                        "error": response.text
                    }
                    
        except Exception as e:
            return {
                "success": False,
                "message": f"Error testing Binance connection: {str(e)}",
                "error": str(e)
            }
    
    async def _get_trading_data(self, client: httpx.AsyncClient, headers: Dict[str, str]) -> Dict[str, Any]:
        """Get additional trading data for validation."""
        trading_data = {
            "open_orders_count": 0,
            "recent_trades_count": 0,
            "positions_count": 0,
            "sample_balances": [],
            "last_activity": "No recent activity"
        }
        
        try:
            timestamp = int(time.time() * 1000)
            
            # Get open orders
            try:
                params = {"timestamp": timestamp}
                signed_query = self.sign_request(params, self.secret_key)
                orders_url = f"{self.base_url}/fapi/v1/openOrders?{signed_query}"
                orders_response = await client.get(orders_url, headers=headers)
                
                if orders_response.status_code == 200:
                    orders = orders_response.json()
                    trading_data["open_orders_count"] = len(orders)
                    
                    # Get sample of recent orders
                    if orders:
                        trading_data["last_activity"] = f"Last order: {orders[0].get('symbol', 'Unknown')} - {orders[0].get('side', 'Unknown')}"
            except:
                pass
            
            # Get recent trades
            try:
                params = {"timestamp": timestamp, "limit": 10}
                signed_query = self.sign_request(params, self.secret_key)
                trades_url = f"{self.base_url}/fapi/v1/userTrades?{signed_query}"
                trades_response = await client.get(trades_url, headers=headers)
                
                if trades_response.status_code == 200:
                    trades = trades_response.json()
                    trading_data["recent_trades_count"] = len(trades)
            except:
                pass
            
            # Get positions
            try:
                params = {"timestamp": timestamp}
                signed_query = self.sign_request(params, self.secret_key)
                positions_url = f"{self.base_url}/fapi/v2/positionRisk?{signed_query}"
                positions_response = await client.get(positions_url, headers=headers)
                
                if positions_response.status_code == 200:
                    positions = positions_response.json()
                    active_positions = [p for p in positions if float(p.get("positionAmt", 0)) != 0]
                    trading_data["positions_count"] = len(active_positions)
            except:
                pass
            
            # Get sample balances (non-zero balances)
            try:
                params = {"timestamp": timestamp}
                signed_query = self.sign_request(params, self.secret_key)
                balance_url = f"{self.base_url}/fapi/v2/balance?{signed_query}"
                balance_response = await client.get(balance_url, headers=headers)
                
                if balance_response.status_code == 200:
                    balances = balance_response.json()
                    non_zero_balances = [
                        {"asset": b.get("asset"), "balance": b.get("balance")} 
                        for b in balances 
                        if float(b.get("balance", 0)) > 0
                    ][:5]  # Show first 5 non-zero balances
                    trading_data["sample_balances"] = non_zero_balances
            except:
                pass
                
        except Exception as e:
            print(f"Error getting trading data: {e}")
        
        return trading_data

class CoinbaseProIntegration(ExchangeIntegration):
    """Coinbase Pro exchange integration."""
    
    def __init__(self, api_key: str, secret_key: str, passphrase: str):
        super().__init__(api_key, secret_key, passphrase)
        self.base_url = "https://api.pro.coinbase.com"
    
    def sign_request(self, method: str, path: str, body: str = "", timestamp: str = None) -> Dict[str, str]:
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
    
    async def test_connection(self) -> Dict[str, Any]:
        """Test Coinbase Pro API key connection with real trading data."""
        try:
            path = "/accounts"
            timestamp = str(int(time.time()))
            
            headers = self.sign_request("GET", path, "", timestamp)
            headers["Content-Type"] = "application/json"
            
            url = f"{self.base_url}{path}"
            
            async with httpx.AsyncClient() as client:
                response = await client.get(url, headers=headers)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Get additional trading data for validation
                    trading_data = await self._get_trading_data(client, headers)
                    
                    return {
                        "success": True,
                        "message": "Coinbase Pro API key connection successful",
                        "exchange_info": {
                            "accounts_count": len(data),
                            "account_types": [acc.get("type") for acc in data[:5]],  # First 5 accounts
                            "open_orders_count": trading_data.get("open_orders_count", 0),
                            "recent_trades_count": trading_data.get("recent_trades_count", 0),
                            "sample_balances": trading_data.get("sample_balances", []),
                            "last_activity": trading_data.get("last_activity", "No recent activity")
                        }
                    }
                else:
                    return {
                        "success": False,
                        "message": f"Coinbase Pro API error: {response.status_code}",
                        "error": response.text
                    }
                    
        except Exception as e:
            return {
                "success": False,
                "message": f"Error testing Coinbase Pro connection: {str(e)}",
                "error": str(e)
            }
    
    async def _get_trading_data(self, client: httpx.AsyncClient, headers: Dict[str, str]) -> Dict[str, Any]:
        """Get additional trading data for validation."""
        trading_data = {
            "open_orders_count": 0,
            "recent_trades_count": 0,
            "sample_balances": [],
            "last_activity": "No recent activity"
        }
        
        try:
            timestamp = str(int(time.time()))
            
            # Get open orders
            try:
                path = "/orders"
                order_headers = self.sign_request("GET", path, "", timestamp)
                order_headers["Content-Type"] = "application/json"
                orders_url = f"{self.base_url}{path}"
                orders_response = await client.get(orders_url, headers=order_headers)
                
                if orders_response.status_code == 200:
                    orders = orders_response.json()
                    trading_data["open_orders_count"] = len(orders)
                    
                    if orders:
                        trading_data["last_activity"] = f"Last order: {orders[0].get('product_id', 'Unknown')} - {orders[0].get('side', 'Unknown')}"
            except:
                pass
            
            # Get recent fills (trades)
            try:
                path = "/fills"
                fills_headers = self.sign_request("GET", path, "", timestamp)
                fills_headers["Content-Type"] = "application/json"
                fills_url = f"{self.base_url}{path}"
                fills_response = await client.get(fills_url, headers=fills_headers)
                
                if fills_response.status_code == 200:
                    fills = fills_response.json()
                    trading_data["recent_trades_count"] = len(fills)
            except:
                pass
            
            # Get account balances
            try:
                path = "/accounts"
                balance_headers = self.sign_request("GET", path, "", timestamp)
                balance_headers["Content-Type"] = "application/json"
                balance_url = f"{self.base_url}{path}"
                balance_response = await client.get(balance_url, headers=balance_headers)
                
                if balance_response.status_code == 200:
                    accounts = balance_response.json()
                    non_zero_balances = [
                        {"currency": acc.get("currency"), "balance": acc.get("balance")} 
                        for acc in accounts 
                        if float(acc.get("balance", 0)) > 0
                    ][:5]  # Show first 5 non-zero balances
                    trading_data["sample_balances"] = non_zero_balances
            except:
                pass
                
        except Exception as e:
            print(f"Error getting Coinbase Pro trading data: {e}")
        
        return trading_data

def get_exchange_integration(exchange: ExchangeType, api_key: str, secret_key: str, passphrase: str = None):
    """Factory function to get the appropriate exchange integration."""
    if exchange == ExchangeType.BINANCE or exchange == ExchangeType.BINANCE_FUTURES:
        return BinanceIntegration(api_key, secret_key, passphrase)
    elif exchange == ExchangeType.COINBASE_PRO:
        return CoinbaseProIntegration(api_key, secret_key, passphrase)
    else:
        raise ValueError(f"Unsupported exchange: {exchange}")

async def test_api_key_connection(exchange: ExchangeType, api_key: str, secret_key: str, passphrase: str = None) -> Dict[str, Any]:
    """Test API key connection for any supported exchange."""
    try:
        integration = get_exchange_integration(exchange, api_key, secret_key, passphrase)
        return await integration.test_connection()
    except Exception as e:
        return {
            "success": False,
            "message": f"Error creating exchange integration: {str(e)}",
            "error": str(e)
        }

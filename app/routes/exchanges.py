from fastapi import APIRouter
from typing import List, Dict, Any
from app.models.api_key import ExchangeType

router = APIRouter()

@router.get("/supported")
async def get_supported_exchanges() -> List[Dict[str, Any]]:
    """Get list of supported exchanges with their information."""
    exchanges = [
        {
            "id": "binance",
            "name": "Binance",
            "slug": "binance",
            "logo": "https://cryptologos.cc/logos/binance-bnb-logo.png",
            "supported_features": ["spot", "futures", "margin"],
            "required_fields": ["apiKey", "secretKey"],
            "test_endpoint": "https://fapi.binance.com/fapi/v2/account",
            "description": "World's largest cryptocurrency exchange by trading volume"
        },
        {
            "id": "binance_futures",
            "name": "Binance Futures",
            "slug": "binance_futures",
            "logo": "https://cryptologos.cc/logos/binance-bnb-logo.png",
            "supported_features": ["futures", "perpetual"],
            "required_fields": ["apiKey", "secretKey"],
            "test_endpoint": "https://fapi.binance.com/fapi/v2/account",
            "description": "Binance's dedicated futures trading platform"
        },
        {
            "id": "coinbase_pro",
            "name": "Coinbase Pro",
            "slug": "coinbase_pro",
            "logo": "https://cryptologos.cc/logos/coinbase-coin-logo.png",
            "supported_features": ["spot"],
            "required_fields": ["apiKey", "secretKey", "passphrase"],
            "test_endpoint": "https://api.pro.coinbase.com/accounts",
            "description": "Professional trading platform by Coinbase"
        },
        {
            "id": "kraken",
            "name": "Kraken",
            "slug": "kraken",
            "logo": "https://cryptologos.cc/logos/kraken-logo.png",
            "supported_features": ["spot", "futures", "margin"],
            "required_fields": ["apiKey", "secretKey"],
            "test_endpoint": "https://api.kraken.com/0/private/Balance",
            "description": "One of the oldest and most trusted exchanges"
        },
        {
            "id": "kucoin",
            "name": "KuCoin",
            "slug": "kucoin",
            "logo": "https://cryptologos.cc/logos/kucoin-token-kcs-logo.png",
            "supported_features": ["spot", "futures", "margin"],
            "required_fields": ["apiKey", "secretKey", "passphrase"],
            "test_endpoint": "https://api.kucoin.com/api/v1/accounts",
            "description": "Popular exchange known for altcoin listings"
        },
        {
            "id": "bybit",
            "name": "Bybit",
            "slug": "bybit",
            "logo": "https://cryptologos.cc/logos/bybit-logo.png",
            "supported_features": ["spot", "futures", "perpetual"],
            "required_fields": ["apiKey", "secretKey"],
            "test_endpoint": "https://api.bybit.com/v2/private/wallet/balance",
            "description": "Leading derivatives exchange with high liquidity"
        },
        {
            "id": "okx",
            "name": "OKX",
            "slug": "okx",
            "logo": "https://cryptologos.cc/logos/okb-okb-logo.png",
            "supported_features": ["spot", "futures", "margin", "options"],
            "required_fields": ["apiKey", "secretKey", "passphrase"],
            "test_endpoint": "https://www.okx.com/api/v5/account/balance",
            "description": "Comprehensive trading platform with advanced features"
        }
    ]
    
    return exchanges

@router.get("/{exchange_slug}/info")
async def get_exchange_info(exchange_slug: str) -> Dict[str, Any]:
    """Get detailed information about a specific exchange."""
    exchanges = await get_supported_exchanges()
    
    for exchange in exchanges:
        if exchange["slug"] == exchange_slug:
            return exchange
    
    return {"error": "Exchange not found"}

@router.get("/{exchange_slug}/features")
async def get_exchange_features(exchange_slug: str) -> Dict[str, Any]:
    """Get supported features for a specific exchange."""
    exchange_info = await get_exchange_info(exchange_slug)
    
    if "error" in exchange_info:
        return exchange_info
    
    return {
        "exchange": exchange_info["name"],
        "features": exchange_info["supported_features"],
        "description": exchange_info["description"]
    }

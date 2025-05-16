import requests

def get_spot_agg_trades(symbol: str, limit: int = 10):
    """
    Fetch aggregate trades from Binance Spot market.
    Endpoint: /api/v3/aggTrades
    """
    url = "https://api.binance.com/api/v3/aggTrades"
    params = {"symbol": symbol.upper(), "limit": limit}
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

def get_futures_agg_trades(symbol: str, limit: int = 10):
    """
    Fetch aggregate trades from Binance Futures market.
    Endpoint: /fapi/v1/aggTrades
    """
    url = "https://fapi.binance.com/fapi/v1/aggTrades"
    params = {"symbol": symbol.upper(), "limit": limit}
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

# Example usage
if __name__ == "__main__":
    spot_trades = get_spot_agg_trades("BTCUSDT", limit=3)
    print("Spot Aggregate Trades:")
    for trade in spot_trades:
        print(trade)

    futures_trades = get_futures_agg_trades("BTCUSDT", limit=3)
    print("\nFutures Aggregate Trades:")
    for trade in futures_trades:
        print(trade)



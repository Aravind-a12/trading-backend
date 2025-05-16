import requests

def get_spot_klines(symbol: str, interval: str, limit: int = 100, start_time: int = None, end_time: int = None):
    """
    Fetch kline/candlestick data from Binance Spot market.
    Endpoint: /api/v3/klines
    """
    url = "https://api.binance.com/api/v3/klines"
    params = {
        "symbol": symbol.upper(),
        "interval": interval,
        "limit": limit
    }
    if start_time is not None:
        params["startTime"] = start_time
    if end_time is not None:
        params["endTime"] = end_time

    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

def get_futures_klines(symbol: str, interval: str, limit: int = 100, start_time: int = None, end_time: int = None):
    """
    Fetch kline/candlestick data from Binance Futures market.
    Endpoint: /fapi/v1/klines
    """
    url = "https://fapi.binance.com/fapi/v1/klines"
    params = {
        "symbol": symbol.upper(),
        "interval": interval,
        "limit": limit
    }
    if start_time is not None:
        params["startTime"] = start_time
    if end_time is not None:
        params["endTime"] = end_time

    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()
    
if __name__ == "__main__":
    print("\nSpot Klines (1h interval):")
    spot_klines = get_spot_klines("BTCUSDT", "1h", limit=3)
    for kline in spot_klines:
        print(kline)

    print("\nFutures Klines (1h interval):")
    futures_klines = get_futures_klines("BTCUSDT", "1h", limit=3)
    for kline in futures_klines:
        print(kline)
        
        
        
import websocket
import json
import threading

def on_message(ws, message):
    data = json.loads(message)
    k = data['k']  # Kline data inside 'k'

    print(f"\n🔔 Kline Update ({'Futures' if 'fstream' in ws.url else 'Spot'}):")
    print(f"Symbol: {data['s']}")
    print(f"Interval: {k['i']}")
    print(f"Open: {k['o']}, High: {k['h']}, Low: {k['l']}, Close: {k['c']}")
    print(f"Volume: {k['v']}")
    print(f"Start Time: {k['t']}, End Time: {k['T']}")
    print(f"Is Final: {k['x']}")

def on_error(ws, error):
    print("❌ Error:", error)

def on_close(ws, close_status_code, close_msg):
    print("❎ WebSocket closed")

def on_open(ws):
    print("✅ WebSocket connection opened")

def run_kline_websocket(symbol, interval, is_futures=False):
    base_url = "wss://fstream.binance.com/ws" if is_futures else "wss://stream.binance.com:9443/ws"
    stream = f"{symbol.lower()}@kline_{interval}"
    ws_url = f"{base_url}/{stream}"

    ws = websocket.WebSocketApp(
        ws_url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )

    thread = threading.Thread(target=ws.run_forever)
    thread.daemon = True
    thread.start()

# Example usage
if __name__ == "__main__":
    run_kline_websocket("BTCUSDT", "1m", is_futures=False)  # Spot
    run_kline_websocket("BTCUSDT", "1m", is_futures=True)   # Futures

    while True:
        pass  # Keep the main thread alive

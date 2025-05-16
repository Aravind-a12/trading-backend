import websocket
import json
import threading
import time

def on_message(ws, message):
    data = json.loads(message)
    print(f"\n🔔 Trade Update ({'Futures' if 'fstream' in ws.url else 'Spot'}):")
    print(f"Symbol: {data['s']}")
    print(f"Price: {data['p']}")
    print(f"Quantity: {data['q']}")
    print(f"Trade Time: {data['T']}")
    print(f"Buyer Maker: {data['m']}")

def on_error(ws, error):
    print(f"❌ Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print(f"🔒 WebSocket closed: {close_status_code} - {close_msg}")

def on_open(ws):
    print(f"🔓 WebSocket opened for {ws.url}")

def start_websocket(symbol: str, is_futures: bool = False):
    """
    Start WebSocket connection for aggregate trades.
    
    :param symbol: Trading pair symbol (e.g., BTCUSDT)
    :param is_futures: True for futures, False for spot
    """
    stream_name = symbol.lower() + "@aggTrade"
    if is_futures:
        url = f"wss://fstream.binance.com/ws/{stream_name}"
    else:
        url = f"wss://stream.binance.com:9443/ws/{stream_name}"

    ws = websocket.WebSocketApp(url,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.on_open = on_open

    thread = threading.Thread(target=ws.run_forever)
    thread.daemon = True
    thread.start()

# 🔄 Run both Spot and Futures WebSocket listeners
if __name__ == "__main__":
    symbol = "btcusdt"

    print("📡 Connecting to Spot and Futures WebSocket Streams for:", symbol.upper())

    start_websocket(symbol, is_futures=False)  # Spot
    start_websocket(symbol, is_futures=True)   # Futures

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("🛑 Exiting WebSocket listener.")

import json
import websocket

SYMBOLS = ["btcusdt", "ethusdt", "bnbusdt", "solusdt", "adausdt"]

stream = "/".join([f"{s}@trade" for s in SYMBOLS])
socket = f"wss://stream.binance.com:9443/stream?streams={stream}"

def on_message(ws, message):
    msg = json.loads(message)
    data = msg["data"]
    price = float(data["p"])
    qty = float(data["q"])
    sym = data["s"]
    print(f"{sym} | Price: {price:.2f} | Qty: {qty}")

def on_open(ws):
    print("Connected and streaming prices...")

def on_error(ws, error):
    print("Error:", error)

def on_close(ws, close_status_code, close_msg):
    print("Connection closed")

ws = websocket.WebSocketApp(
    socket,
    on_open=on_open,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close,
)

ws.run_forever()
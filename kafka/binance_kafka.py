import json
import logging
import os
from datetime import datetime, timezone

import websocket
from confluent_kafka import Producer

from common import kafka_producer_config


logging.basicConfig(level=logging.INFO)

producer = Producer(kafka_producer_config("binance-trades-producer"))
TOPIC = os.getenv("KAFKA_TRADES_TOPIC", "trades_topic")

SYMBOLS = [
    symbol.strip().lower()
    for symbol in os.getenv("BINANCE_SYMBOLS", "btcusdt,ethusdt,bnbusdt,solusdt,adausdt").split(",")
    if symbol.strip()
]
stream = "/".join([f"{symbol}@trade" for symbol in SYMBOLS])
socket_url = f"wss://stream.binance.com:9443/stream?streams={stream}"


def delivery_report(err, msg):
    if err is not None:
        logging.error("Message delivery failed: %s", err)
    else:
        logging.info("Message delivered to %s [%s]", msg.topic(), msg.partition())


def on_message(ws, message):
    try:
        msg = json.loads(message)
        data = msg["data"]

        trade_data = {
            "symbol": data["s"],
            "price": float(data["p"]),
            "quantity": float(data["q"]),
            "trade_id": int(data["t"]),
            "event_time": datetime.fromtimestamp(data["T"] / 1000, tz=timezone.utc).isoformat(),
            "ingestion_time": datetime.now(timezone.utc).isoformat(),
            "source": "Binance",
        }

        producer.produce(
            topic=TOPIC,
            key=trade_data["symbol"],
            value=json.dumps(trade_data),
            callback=delivery_report,
        )
        producer.poll(0)

    except Exception:
        logging.exception("Error processing Binance trade message")


def on_open(ws):
    logging.info("Connected to Binance WebSocket: %s", ",".join(SYMBOLS))


def on_error(ws, error):
    logging.error("WebSocket error: %s", error)


def on_close(ws, close_status_code, close_msg):
    logging.warning("WebSocket connection closed: %s %s", close_status_code, close_msg)
    producer.flush()


if __name__ == "__main__":
    ws = websocket.WebSocketApp(
        socket_url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    ws.run_forever()

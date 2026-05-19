import json
import os
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer


FRED_API_KEY = os.getenv("FRED_API_KEY")
TOPIC = os.getenv("KAFKA_FRED_TOPIC", "fred_topic")
START_DATE = os.getenv("FRED_OBSERVATION_START", "2026-01-01")

DEFAULT_SERIES = {
    "DFF": "Federal Funds Rate",
    "CPIAUCSL": "Consumer Price Index",
    "UNRATE": "Unemployment Rate",
    "GDP": "Gross Domestic Product",
    "PCE": "Personal Consumption Expenditures",
    "M2SL": "M2 Money Stock",
    "PAYEMS": "All Employees, Total Nonfarm",
    "INDPRO": "Industrial Production Index",
    "FEDFUNDS": "Effective Federal Funds Rate",
    "TB3MS": "3-Month Treasury Bill Rate",
}

SERIES_LIST = {
    series_id.strip(): DEFAULT_SERIES.get(series_id.strip(), series_id.strip())
    for series_id in os.getenv("FRED_SERIES", ",".join(DEFAULT_SERIES)).split(",")
    if series_id.strip()
}

KAFKA_CONFIG = {
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    "security.protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
    "sasl.mechanisms": os.getenv("KAFKA_SASL_MECHANISMS", "PLAIN"),
    "sasl.username": os.getenv("KAFKA_SASL_USERNAME"),
    "sasl.password": os.getenv("KAFKA_SASL_PASSWORD"),
    "client.id": "fred-airflow-producer",
}


def require_config() -> None:
    missing = [key for key, value in KAFKA_CONFIG.items() if key in {"bootstrap.servers", "sasl.username", "sasl.password"} and not value]
    if missing:
        raise RuntimeError(f"Missing Kafka configuration values: {', '.join(missing)}")
    if not FRED_API_KEY:
        raise RuntimeError("Missing required environment variable: FRED_API_KEY")


def fetch_fred_series(series_id: str, series_name: str) -> list[dict]:
    response = requests.get(
        "https://api.stlouisfed.org/fred/series/observations",
        params={
            "series_id": series_id,
            "api_key": FRED_API_KEY,
            "file_type": "json",
            "observation_start": START_DATE,
        },
        timeout=30,
    )
    response.raise_for_status()

    records = []
    for observation in response.json().get("observations", []):
        if observation.get("value") == ".":
            continue
        records.append(
            {
                "series_id": series_id,
                "series_name": series_name,
                "date": observation["date"],
                "value": float(observation["value"]),
                "source": "FRED",
                "ingestion_time": datetime.now(timezone.utc).isoformat(),
            }
        )
    return records


def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()}")


def main() -> None:
    require_config()
    producer = Producer(KAFKA_CONFIG)

    for series_id, series_name in SERIES_LIST.items():
        print(f"Fetching {series_id}...")
        for record in fetch_fred_series(series_id, series_name):
            producer.produce(
                TOPIC,
                key=record["series_id"],
                value=json.dumps(record),
                callback=delivery_report,
            )
            producer.poll(0)

    producer.flush()
    print("All FRED records sent to Kafka.")


if __name__ == "__main__":
    main()

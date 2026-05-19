import json
import logging
import os
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer

from common import kafka_producer_config


logging.basicConfig(level=logging.INFO)

producer = Producer(kafka_producer_config("newsapi-producer"))
TOPIC = os.getenv("KAFKA_NEWS_TOPIC", "news_topic")

NEWS_URL = "https://newsapi.org/v2/everything"
NEWS_PARAMS = {
    "q": os.getenv("NEWS_QUERY", '(bitcoin OR ethereum) AND (price OR trading OR market OR volatility)'),
    "sources": os.getenv("NEWS_SOURCES", "reuters,bloomberg,cnbc,cointelegraph,financial-times"),
    "language": "en",
    "sortBy": "publishedAt",
    "pageSize": 5,
    "apiKey": os.getenv("NEWS_API_KEY"),
}


def delivery_report(err, msg):
    if err is not None:
        logging.error("Message delivery failed: %s", err)
    else:
        logging.info("Message delivered to %s [%s]", msg.topic(), msg.partition())


def fetch_and_publish_news():
    if not NEWS_PARAMS["apiKey"]:
        raise RuntimeError("Missing required environment variable: NEWS_API_KEY")

    response = requests.get(NEWS_URL, params=NEWS_PARAMS, timeout=30)
    response.raise_for_status()
    data = response.json()

    if data.get("status") != "ok":
        logging.error("NewsAPI error: %s", data)
        return

    for article in data.get("articles", []):
        news_data = {
            "title": article.get("title"),
            "description": article.get("description"),
            "url": article.get("url"),
            "source": article.get("source", {}).get("name"),
            "published_at": article.get("publishedAt"),
            "ingestion_time": datetime.now(timezone.utc).isoformat(),
        }
        producer.produce(
            topic=TOPIC,
            key=news_data["source"] or "unknown",
            value=json.dumps(news_data),
            callback=delivery_report,
        )
        producer.poll(0)

    producer.flush()
    logging.info("Fetched and published %s articles", len(data.get("articles", [])))


if __name__ == "__main__":
    while True:
        try:
            fetch_and_publish_news()
        except Exception:
            logging.exception("Error fetching or publishing news")
        time.sleep(int(os.getenv("NEWS_POLL_SECONDS", "60")))

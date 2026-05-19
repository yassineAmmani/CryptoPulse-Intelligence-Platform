import os
from datetime import datetime, timezone

import requests


API_KEY = os.getenv("NEWS_API_KEY")


def fetch_news():
    if not API_KEY:
        raise RuntimeError("Missing required environment variable: NEWS_API_KEY")

    response = requests.get(
        "https://newsapi.org/v2/everything",
        params={
            "q": os.getenv("NEWS_QUERY", '(bitcoin OR ethereum) AND (price OR trading OR market OR volatility)'),
            "sources": os.getenv("NEWS_SOURCES", "reuters,bloomberg,cnbc,cointelegraph,financial-times"),
            "language": "en",
            "sortBy": "publishedAt",
            "pageSize": 5,
            "apiKey": API_KEY,
        },
        timeout=30,
    )

    if response.status_code != 200:
        print("HTTP Error:", response.status_code)
        print(response.text)
        return

    data = response.json()
    if data.get("status") != "ok":
        print("API Error:", data)
        return

    print("\n====== Crypto & Market News ======\n")
    for article in data.get("articles", []):
        print(f"Title        : {article.get('title')}")
        print(f"Source       : {article.get('source', {}).get('name')}")
        print(f"Published At : {article.get('publishedAt')}")
        print(f"Description  : {article.get('description')}")
        print("-" * 60)

    print("\nFetched at:", datetime.now(timezone.utc).isoformat())
    print("Total matching results:", data.get("totalResults"))


if __name__ == "__main__":
    fetch_news()

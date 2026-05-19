import os

import pandas as pd
import requests


FRED_API_KEY = os.getenv("FRED_API_KEY")

SERIES_LIST = {
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

START_DATE = os.getenv("FRED_OBSERVATION_START", "2026-01-01")


def fetch_fred_series(series_id, series_name, start_date):
    if not FRED_API_KEY:
        raise RuntimeError("Missing required environment variable: FRED_API_KEY")

    response = requests.get(
        "https://api.stlouisfed.org/fred/series/observations",
        params={
            "series_id": series_id,
            "api_key": FRED_API_KEY,
            "file_type": "json",
            "observation_start": start_date,
        },
        timeout=30,
    )
    response.raise_for_status()

    observations = response.json().get("observations", [])
    if not observations:
        print(f"No data returned for {series_id}")
        return pd.DataFrame(columns=["series_id", "series_name", "date", "value"])

    df = pd.DataFrame(observations)
    df["date"] = pd.to_datetime(df["date"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["series_id"] = series_id
    df["series_name"] = series_name
    return df[["series_id", "series_name", "date", "value"]]


if __name__ == "__main__":
    all_data = pd.concat(
        [fetch_fred_series(k, v, START_DATE) for k, v in SERIES_LIST.items()],
        ignore_index=True,
    )
    all_data.to_csv("fred_all_indicators.csv", index=False)
    print(all_data.head(20))

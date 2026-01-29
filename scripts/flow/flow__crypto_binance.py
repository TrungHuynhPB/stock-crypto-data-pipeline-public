"""
Prefect flow: Fetch latest crypto prices from Binance, CSV -> MinIO -> Snowflake stage -> Postgres
- Reads cryptos from seeds/cryptolist.txt
- Pairs with USD (uses USDT market as proxy), captures price and 24h volume
- Writes data/crypto_binance_{data_date}.csv
- Uploads to MinIO s3://minio-stock-bucket/raw-data/crypto/binance/
- PUT to Snowflake stage (schema.stage)
- Loads CSV rows into local Postgres table raw_cryptoprices_binance
"""
from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import List

import pandas as pd
import requests
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger

from scripts.flow.crypto_prices_common import (
    load_crypto_list,
    save_source_csv,
    upload_minio_and_stage,
    ensure_postgres_table,
    load_csv_to_postgres,
)

load_dotenv()

BINANCE_URL = os.getenv("BINANCE_URL", "https://api.binance.com/api/v3")


@task(name="Fetch Binance Prices")
def fetch_binance(cryptos: List[str]) -> pd.DataFrame:
    logger = get_run_logger()
    rows = []
    try:
        resp = requests.get(f"{BINANCE_URL}/ticker/24hr", timeout=30)
        resp.raise_for_status()
        data = resp.json()
        now_iso = datetime.now().isoformat()
        for t in data:
            symbol = t.get("symbol", "")
            # Prefer USDT market as proxy for USD
            if symbol.endswith("USDT"):
                base = symbol[:-4].lower()
                if base in cryptos:
                    price = float(t.get("lastPrice", 0) or 0)
                    volume = float(t.get("volume", 0) or 0)
                    rows.append({
                        "symbol": f"{base.upper()}-USD",
                        "base_currency": base,
                        "quote_currency": "USD",
                        "price": price,
                        "volume": volume,
                        "source": "binance",
                        "observed_at": now_iso,
                    })
        logger.info(f"Fetched {len(rows)} Binance rows")
    except Exception as e:
        logger.error(f"Binance fetch error: {e}")
        raise
    return pd.DataFrame(rows)


@flow(name="crypto_prices__binance")
def crypto_prices_binance_flow() -> Path:
    logger = get_run_logger()
    logger.info("🚀 Start Binance crypto prices flow")

    cryptos = load_crypto_list()
    df = fetch_binance(cryptos)

    if df.empty:
        raise ValueError("No Binance data returned")

    csv_path = save_source_csv(df, source="binance")

    ok = upload_minio_and_stage(csv_path, source="binance")
    if not ok:
        raise RuntimeError("Upload to MinIO/Stage failed")

    ensure_postgres_table("binance")
    load_csv_to_postgres(csv_path, "binance")

    logger.info("✅ Binance flow completed")
    return csv_path


if __name__ == "__main__":
    crypto_prices_binance_flow()

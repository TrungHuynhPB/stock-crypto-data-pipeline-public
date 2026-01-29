"""
Prefect flow: Fetch latest crypto prices from CoinGecko, CSV -> MinIO -> Snowflake stage -> Postgres
- Reads cryptos from seeds/cryptolist.txt
- Uses /simple/price with vs_currency=usd to get price and 24h volume
- Writes data/crypto_coingecko_{data_date}.csv
- Uploads to MinIO and stages file to Snowflake
- Loads CSV rows into local Postgres table raw_cryptoprices_coingecko
"""
from __future__ import annotations

import os
import time
from datetime import datetime
from pathlib import Path
from typing import List

import pandas as pd
import requests
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger

from scripts.data_generation.a2_0_crypto_common import (
    load_crypto_list,
    save_source_csv,
    upload_minio_and_stage,
    ensure_postgres_table,
    load_csv_to_postgres,
    ensure_snowflake_table,
    load_csv_into_snowflake,
)

load_dotenv()

COINGECKO_URL = os.getenv("COINGECKO_URL", "https://api.coingecko.com/api/v3")


@task(name="Fetch CoinGecko Prices")
def fetch_coingecko(cryptos: List[str]) -> pd.DataFrame:
    logger = get_run_logger()
    rows = []
    batch_size = 50
    try:
        for i in range(0, len(cryptos), batch_size):
            batch = cryptos[i:i + batch_size]
            coin_ids = ",".join(batch)
            params = {
                "ids": coin_ids,
                "vs_currencies": "usd",
                "include_24hr_vol": "true",
                "include_last_updated_at": "true",
            }
            r = requests.get(f"{COINGECKO_URL}/simple/price", params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            now_iso = datetime.now().isoformat()
            for coin_id, coin_data in data.items():
                price = coin_data.get("usd")
                vol = coin_data.get("usd_24h_vol")
                if price is None:
                    continue
                base = coin_id.lower()
                rows.append({
                    "symbol": f"{base.upper()}-USD",
                    "base_currency": base,
                    "quote_currency": "USD",
                    "price": float(price),
                    "volume": float(vol or 0),
                    "source": "coingecko",
                    "observed_at": now_iso,
                })
            # gentle rate limiting
            if i + batch_size < len(cryptos):
                time.sleep(1)
        logger.info(f"Fetched {len(rows)} CoinGecko rows")
    except Exception as e:
        logger.error(f"CoinGecko fetch error: {e}")
        raise
    return pd.DataFrame(rows)


@flow(name="a2_crypto_prices__coingecko")
def crypto_prices_coingecko_flow() -> Path:
    logger = get_run_logger()
    logger.info("🚀 Start CoinGecko crypto prices flow")

    cryptos = load_crypto_list()
    df = fetch_coingecko(cryptos)

    if df.empty:
        raise ValueError("No CoinGecko data returned")

    csv_path = save_source_csv(df, source="coingecko")

    ok = upload_minio_and_stage(csv_path, source="coingecko")
    if not ok:
        raise RuntimeError("Upload to MinIO/Stage failed")

    ensure_postgres_table("coingecko")
    load_csv_to_postgres(csv_path, "coingecko")
    ensure_snowflake_table("coingecko")
    load_csv_into_snowflake(csv_path, "coingecko")

    logger.info("✅ CoinGecko flow completed")
    return csv_path


if __name__ == "__main__":
    crypto_prices_coingecko_flow()

"""
Prefect flow: Fetch latest crypto prices from Yahoo Finance (yfinance),
CSV -> MinIO -> Snowflake stage -> Postgres
- Reads cryptos from seeds/cryptolist.txt
- Queries tickers as BASE-USD (e.g., BTC-USD)
- Uses most recent bar from 1d/1m history as spot proxy; volume from same bar
- Writes data/crypto_yfinance_{data_date}.csv
- Uploads to MinIO and stages file to Snowflake
- Loads CSV rows into local Postgres table raw_cryptoprices_yfinance
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import List

import pandas as pd
import yfinance as yf
from prefect import flow, task, get_run_logger
from dotenv import load_dotenv

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


@task(name="Fetch YFinance Prices")
def fetch_yfinance(cryptos: List[str]) -> pd.DataFrame:
    logger = get_run_logger()
    rows = []
    now_iso = datetime.now().isoformat()

    for base in cryptos:
        ticker = f"{base.upper()}-USD"
        try:
            t = yf.Ticker(ticker)
            # Use short window to avoid heavy downloads
            hist = t.history(period="1d", interval="1m")
            if hist is None or hist.empty:
                # Fallback to 1d daily interval
                hist = t.history(period="1d", interval="1d")
            if hist is None or hist.empty:
                continue
            last = hist.tail(1)
            price = float(last["Close"].iloc[0]) if "Close" in last.columns else None
            volume = float(last["Volume"].iloc[0]) if "Volume" in last.columns else 0.0
            if price is None:
                continue
            rows.append({
                "symbol": f"{base.upper()}-USD",
                "base_currency": base,
                "quote_currency": "USD",
                "price": price,
                "volume": volume,
                "source": "yfinance",
                "observed_at": now_iso,
            })
        except Exception as e:
            logger.warning(f"yfinance error for {ticker}: {e}")
            continue

    logger.info(f"Fetched {len(rows)} yfinance rows")
    return pd.DataFrame(rows)


@flow(name="a2_crypto_prices__yfinance")
def crypto_prices_yfinance_flow() -> Path:
    logger = get_run_logger()
    logger.info("🚀 Start yfinance crypto prices flow")

    cryptos = load_crypto_list()
    df = fetch_yfinance(cryptos)

    if df.empty:
        raise ValueError("No yfinance data returned")

    csv_path = save_source_csv(df, source="yfinance")

    ok = upload_minio_and_stage(csv_path, source="yfinance")
    if not ok:
        raise RuntimeError("Upload to MinIO/Stage failed")

    ensure_postgres_table("yfinance")
    load_csv_to_postgres(csv_path, "yfinance")
    ensure_snowflake_table("yfinance")
    load_csv_into_snowflake(csv_path, "yfinance")

    logger.info("✅ yfinance flow completed")
    return csv_path


if __name__ == "__main__":
    crypto_prices_yfinance_flow()

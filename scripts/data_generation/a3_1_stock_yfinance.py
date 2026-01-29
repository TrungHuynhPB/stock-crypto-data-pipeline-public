""" 
Prefect flow: Fetch daily stock prices + company metadata from Yahoo Finance (yfinance)
CSV -> MinIO -> Snowflake stage -> Postgres -> Snowflake raw table

- Reads stock tickers from seeds/stocklist.txt
- For each ticker, pulls the most recent daily OHLCV bar via `history(period="5d", interval="1d")`
  and uses the last row as latest trading day record.
- Enriches with company metadata from `Ticker.info` (company name, sector, industry, etc.)
- Writes data/stock_yfinance_{data_date}.csv
- Uploads to MinIO under raw-data/stock/yfinance/
- PUTs CSV to Snowflake stage
- Loads CSV into Postgres table raw_stock_historical_prices_yfinance
- Ensures + loads Snowflake table RAW_STOCK_HISTORICAL_PRICES_YFINANCE
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import List

import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
from prefect import flow, get_run_logger, task

from scripts.data_generation.a3_0_stock_common import (
    ensure_postgres_table,
    ensure_snowflake_table,
    load_csv_into_snowflake,
    load_csv_to_postgres,
    load_stock_list,
    save_source_csv,
    upload_minio_and_stage,
)

load_dotenv()


def _safe_float(v):
    try:
        if v is None:
            return None
        # yfinance sometimes returns numpy types
        return float(v)
    except Exception:
        return None


@task(name="Fetch Stock Daily Prices from YFinance")
def fetch_yfinance(tickers: List[str]) -> pd.DataFrame:
    logger = get_run_logger()
    rows: list[dict] = []

    now_iso = datetime.now().isoformat()

    for ticker in tickers:
        try:
            t = yf.Ticker(ticker)

            # recent daily bars; last row = latest trading day
            hist = t.history(period="5d", interval="1d")
            if hist is None or hist.empty:
                logger.warning(f"No history returned for {ticker}")
                continue

            last = hist.tail(1)

            # yfinance index is timezone aware sometimes; normalize to date string
            last_idx = last.index[-1]
            date_val = getattr(last_idx, "date", lambda: last_idx)()

            open_price = _safe_float(last["Open"].iloc[0]) if "Open" in last.columns else None
            high_price = _safe_float(last["High"].iloc[0]) if "High" in last.columns else None
            low_price = _safe_float(last["Low"].iloc[0]) if "Low" in last.columns else None
            close_price = _safe_float(last["Close"].iloc[0]) if "Close" in last.columns else None
            adj_close_price = _safe_float(last["Close"].iloc[0])
            if "Adj Close" in last.columns:
                adj_close_price = _safe_float(last["Adj Close"].iloc[0])

            volume = _safe_float(last["Volume"].iloc[0]) if "Volume" in last.columns else None
            dividends = _safe_float(last["Dividends"].iloc[0]) if "Dividends" in last.columns else 0.0
            stock_splits = _safe_float(last["Stock Splits"].iloc[0]) if "Stock Splits" in last.columns else 0.0

            # company metadata
            info = {}
            try:
                info = t.info or {}
            except Exception as e:
                logger.warning(f"Failed reading info for {ticker}: {e}")

            company_name = info.get("shortName") or info.get("longName")
            sector = info.get("sector")
            industry = info.get("industry")
            market_cap = _safe_float(info.get("marketCap"))
            pe_ratio = _safe_float(info.get("trailingPE") or info.get("forwardPE"))
            week_52_high = _safe_float(info.get("fiftyTwoWeekHigh"))
            week_52_low = _safe_float(info.get("fiftyTwoWeekLow"))
            avg_volume = _safe_float(info.get("averageVolume") or info.get("averageVolume10days"))

            rows.append(
                {
                    "ticker": ticker,
                    "date": str(date_val),
                    "open_price": open_price,
                    "high_price": high_price,
                    "low_price": low_price,
                    "close_price": close_price,
                    "adj_close_price": adj_close_price,
                    "volume": volume,
                    "dividends": dividends,
                    "stock_splits": stock_splits,
                    "company_name": company_name,
                    "sector": sector,
                    "industry": industry,
                    "market_cap": market_cap,
                    "pe_ratio": pe_ratio,
                    "week_52_high": week_52_high,
                    "week_52_low": week_52_low,
                    "avg_volume": avg_volume,
                    "source": "yfinance",
                    "observed_at": now_iso,
                }
            )
        except Exception as e:
            logger.warning(f"yfinance error for {ticker}: {e}")
            continue

    logger.info(f"Fetched {len(rows)} yfinance stock rows")
    return pd.DataFrame(rows)


@flow(name="a3_stock_prices__yfinance")
def stock_prices_yfinance_flow() -> Path:
    logger = get_run_logger()
    logger.info("🚀 Start yfinance stock prices flow")

    tickers = load_stock_list()
    df = fetch_yfinance(tickers)

    if df.empty:
        raise ValueError("No yfinance stock data returned")

    csv_path = save_source_csv(df, source="yfinance")

    ok = upload_minio_and_stage(csv_path, source="yfinance")
    if not ok:
        raise RuntimeError("Upload to MinIO/Stage failed")

    ensure_postgres_table("yfinance")
    load_csv_to_postgres(csv_path, "yfinance")

    ensure_snowflake_table("yfinance")
    load_csv_into_snowflake(csv_path, "yfinance")

    logger.info("✅ yfinance stock flow completed")
    return csv_path


if __name__ == "__main__":
    stock_prices_yfinance_flow()

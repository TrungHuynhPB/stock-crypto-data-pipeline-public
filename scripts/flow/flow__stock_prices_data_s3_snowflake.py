"""
Prefect orchestration flow to run all a3_ stock price ingestion subflows
and stage their CSVs to MinIO/Snowflake while loading into Postgres + Snowflake.

Subflows executed (imported from scripts.data_generation):
- a3_1_stock_yfinance.stock_prices_yfinance_flow

Each subflow:
- reads seeds/stocklist.txt
- fetches latest daily price + some metadata
- writes data/stock_{source}_YYYYMMDD_HHMMSS.csv
- uploads to MinIO and PUTs to Snowflake stage
- loads CSV rows into Postgres raw_stock_prices_{source}
- loads CSV rows into Snowflake RAW_STOCK_PRICES_{SOURCE}

Usage:
  python3 -m scripts.flow.flow__stock_prices_data_s3_snowflake
  # or run individual subflows directly
  python3 -m scripts.data_generation.a3_1_stock_yfinance
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from prefect import flow, get_run_logger, task

from scripts.data_generation.a3_1_stock_yfinance import stock_prices_yfinance_flow


@task(name="Run YFinance Stock Subflow")
def run_yfinance() -> Optional[Path]:
    return stock_prices_yfinance_flow()


@flow(name="flow__stock_prices_data_s3_snowflake")
def stock_prices_data_s3_snowflake_flow(
    sources: Optional[list[str]] = None,
) -> dict[str, Optional[str]]:
    """Run selected a3_ stock price subflows sequentially.

    Args:
        sources: list of sources to run from {yfinance}. If None, runs all.

    Returns:
        dict mapping source -> CSV local path (as string) or None if failed/skipped
    """

    logger = get_run_logger()

    all_sources = ["yfinance"]
    sources = sources or all_sources

    results: dict[str, Optional[str]] = {s: None for s in all_sources}

    logger.info(f"🚀 Starting a3 stock prices orchestration for: {sources}")

    if "yfinance" in sources:
        try:
            p = run_yfinance()
            results["yfinance"] = str(p) if p else None
        except Exception as e:
            logger.error(f"❌ yfinance stock subflow failed: {e}")

    logger.info("✅ a3 stock prices orchestration completed")
    return results


if __name__ == "__main__":
    stock_prices_data_s3_snowflake_flow()

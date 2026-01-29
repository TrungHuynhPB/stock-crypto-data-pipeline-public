"""
Prefect orchestration flow to run all a2_ crypto price ingestion subflows
and stage their CSVs to MinIO/Snowflake while loading into local Postgres.

Subflows executed (imported from scripts.data_generation):
- a2_1_crypto_binance.crypto_prices_binance_flow
- a2_2_crypto_coingecko.crypto_prices_coingecko_flow
- a2_3_crypto_yfinance.crypto_prices_yfinance_flow
- a2_4_crypto_freecryptoapi.crypto_prices_freecryptoapi_flow

Each subflow:
- reads seeds/cryptolist.txt
- fetches latest USD-paired prices/volumes
- writes data/crypto_{source}_YYYYMMDD_HHMMSS.csv
- uploads to MinIO and PUTs to Snowflake stage
- loads CSV rows into local Postgres raw_cryptoprices_{source}

Usage:
  python3 -m scripts.flow.flow__prices_data_s3_snowflake
  # or run individual subflows directly, e.g.:
  python3 -m scripts.data_generation.a2_1_crypto_binance
"""


from pathlib import Path
from typing import Optional

from prefect import flow, task, get_run_logger

# Import subflows
from scripts.data_generation.a2_1_crypto_binance import crypto_prices_binance_flow
from scripts.data_generation.a2_2_crypto_coingecko import crypto_prices_coingecko_flow
from scripts.data_generation.a2_3_crypto_yfinance import crypto_prices_yfinance_flow
from scripts.data_generation.a2_4_crypto_freecryptoapi import crypto_prices_freecryptoapi_flow


@task(name="Run Binance Subflow")
def run_binance() -> Optional[Path]:
    return crypto_prices_binance_flow()


@task(name="Run CoinGecko Subflow")
def run_coingecko() -> Optional[Path]:
    return crypto_prices_coingecko_flow()


@task(name="Run YFinance Subflow")
def run_yfinance() -> Optional[Path]:
    return crypto_prices_yfinance_flow()


@task(name="Run FreeCryptoAPI Subflow")
def run_freecryptoapi() -> Optional[Path]:
    return crypto_prices_freecryptoapi_flow()


@flow(name="flow__prices_data_s3_snowflake")
def prices_data_s3_snowflake_flow(sources: Optional[list[str]] = None) -> dict[str, Optional[str]]:
    """Run selected a2_ crypto price subflows sequentially.

    Args:
        sources: list of sources to run from {binance, coingecko, yfinance, freecryptoapi}.
                 If None, runs all four.

    Returns:
        dict mapping source -> CSV local path (as string) or None if failed/skipped
    """
    logger = get_run_logger()
    all_sources = ["binance", "coingecko", "yfinance", "freecryptoapi"]
    sources = sources or all_sources

    results: dict[str, Optional[str]] = {s: None for s in all_sources}

    try:
        logger.info(f"🚀 Starting a2 crypto prices orchestration for: {sources}")

        if "binance" in sources:
            try:
                p = run_binance()
                results["binance"] = str(p) if p else None
            except Exception as e:
                logger.error(f"❌ Binance subflow failed: {e}")

        if "coingecko" in sources:
            try:
                p = run_coingecko()
                results["coingecko"] = str(p) if p else None
            except Exception as e:
                logger.error(f"❌ CoinGecko subflow failed: {e}")

        if "yfinance" in sources:
            try:
                p = run_yfinance()
                results["yfinance"] = str(p) if p else None
            except Exception as e:
                logger.error(f"❌ yfinance subflow failed: {e}")

        if "freecryptoapi" in sources:
            try:
                p = run_freecryptoapi()
                results["freecryptoapi"] = str(p) if p else None
            except Exception as e:
                logger.error(f"⚠️ FreeCryptoAPI subflow failed or skipped: {e}")

        logger.info("✅ a2 crypto prices orchestration completed")
        return results

    except Exception as e:
        logger.error(f"❌ Orchestration flow failed: {e}")
        raise


if __name__ == "__main__":
    prices_data_s3_snowflake_flow()

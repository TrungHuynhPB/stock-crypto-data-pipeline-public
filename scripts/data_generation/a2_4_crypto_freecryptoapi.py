"""
Prefect flow: Fetch latest crypto prices from FreeCryptoAPI (generic),
CSV -> MinIO -> Snowflake stage -> Postgres

Configuration via environment variables:
- FREECRYPTO_API: Base URL or templated endpoint containing "{symbol}" placeholder.
  Examples:
    https://api.freecryptoapi.com/v1/price?symbol={symbol}&convert=USD
    https://my-gateway.example/price/{symbol}?convert=USD
- FREECRYPTO_API_KEY (optional): API key to send either as ?apikey=... or headers
- FREECRYPTO_API_HEADER (optional): Custom header name to carry the API key (default: x-api-key)

Notes:
- We attempt to extract fields (price, volume) from flexible JSON responses.
- If the base URL in FREECRYPTO_API does not contain {symbol}, we will call
  {base}/price?symbol={symbol}&convert=USD
"""
from __future__ import annotations

import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

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

BASE_URL = os.getenv("FREECRYPTO_API", "").strip()
API_KEY = os.getenv("FREECRYPTO_API_KEY", "").strip()
API_KEY_HEADER = os.getenv("FREECRYPTO_API_HEADER", "x-api-key").strip() or "x-api-key"


def _extract_price_volume(obj: Any) -> (Optional[float], Optional[float]):
    """Try to extract price and volume from various JSON shapes recursively."""
    price_keys = ["price", "last_price", "last", "rate", "usd", "close"]
    volume_keys = ["volume_24h", "vol24h", "volume", "usd_24h_vol", "quoteVolume"]

    if isinstance(obj, dict):
        # direct keys
        price = next((obj.get(k) for k in price_keys if k in obj), None)
        vol = next((obj.get(k) for k in volume_keys if k in obj), None)
        # normalize
        try:
            price_f = float(price) if price is not None else None
        except Exception:
            price_f = None
        try:
            vol_f = float(vol) if vol is not None else None
        except Exception:
            vol_f = None
        if price_f is not None or vol_f is not None:
            return price_f, vol_f
        # search nested
        for v in obj.values():
            p, q = _extract_price_volume(v)
            if p is not None or q is not None:
                return p, q
    elif isinstance(obj, list):
        for v in obj:
            p, q = _extract_price_volume(v)
            if p is not None or q is not None:
                return p, q
    return None, None


def _build_url_for_symbol(base: str, symbol: str) -> str:
    if "{symbol}" in base:
        return base.format(symbol=symbol)
    base = base.rstrip("/")
    return f"{base}/price?symbol={symbol}&convert=USD"


@task(name="Fetch FreeCryptoAPI Prices")
def fetch_freecryptoapi(cryptos: List[str]) -> pd.DataFrame:
    logger = get_run_logger()
    if not BASE_URL:
        logger.warning("FREECRYPTO_API is not set; skipping FreeCryptoAPI flow")
        return pd.DataFrame([])

    headers = {"Accept": "application/json"}
    if API_KEY:
        # Prefer header, but some APIs use query param; we'll add as header first
        headers[API_KEY_HEADER] = API_KEY
        headers.setdefault("Authorization", f"Bearer {API_KEY}")

    rows = []
    for base in cryptos:
        sym = base.upper()
        url = _build_url_for_symbol(BASE_URL, sym)
        try:
            r = requests.get(url, headers=headers, timeout=20)
            # If key must be query param
            if r.status_code == 401 and API_KEY:
                r = requests.get(url + ("&" if "?" in url else "?") + f"apikey={API_KEY}", timeout=20)
            r.raise_for_status()
            data = r.json()
            price, volume = _extract_price_volume(data)
            if price is None:
                logger.debug(f"No price parsed for {sym} from {url}")
                continue
            rows.append({
                "symbol": f"{sym}-USD",
                "base_currency": base.lower(),
                "quote_currency": "USD",
                "price": float(price),
                "volume": float(volume or 0.0),
                "source": "freecryptoapi",
                "observed_at": datetime.now().isoformat(),
            })
        except Exception as e:
            logger.warning(f"FreeCryptoAPI error for {sym}: {e}")
            continue
        # small sleep to be gentle
        time.sleep(0.1)

    logger.info(f"Fetched {len(rows)} FreeCryptoAPI rows")
    return pd.DataFrame(rows)


@flow(name="a2_crypto_prices__freecryptoapi")
def crypto_prices_freecryptoapi_flow() -> Path:
    logger = get_run_logger()
    logger.info("🚀 Start FreeCryptoAPI crypto prices flow")

    cryptos = load_crypto_list()
    df = fetch_freecryptoapi(cryptos)

    if df.empty:
        logger.warning("No FreeCryptoAPI data returned; flow will still succeed with empty dataset")
        # Still create an empty CSV to mark run
        csv_path = save_source_csv(pd.DataFrame(columns=[
            "symbol", "base_currency", "quote_currency", "price", "volume", "source", "observed_at"
        ]), source="freecryptoapi")
        return csv_path

    csv_path = save_source_csv(df, source="freecryptoapi")

    ok = upload_minio_and_stage(csv_path, source="freecryptoapi")
    if not ok:
        raise RuntimeError("Upload to MinIO/Stage failed")

    ensure_postgres_table("freecryptoapi")
    load_csv_to_postgres(csv_path, "freecryptoapi")
    ensure_snowflake_table("freecryptoapi")
    load_csv_into_snowflake(csv_path, "freecryptoapi")    

    logger.info("✅ FreeCryptoAPI flow completed")
    return csv_path


if __name__ == "__main__":
    crypto_prices_freecryptoapi_flow()

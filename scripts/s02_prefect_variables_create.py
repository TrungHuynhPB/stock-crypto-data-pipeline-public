#!/usr/bin/env python3
import os
import logging
from prefect.variables import Variable
from dotenv import load_dotenv

os.environ['PREFECT_API_URL'] = os.getenv("PREFECT_API_URL")
# -------------------------------------------------------------------
# Logging setup (bash / CI friendly)
# -------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("prefect-vars")

# -------------------------------------------------------------------
# Variables to create (defaults)
# -------------------------------------------------------------------
VARIABLES = {
    "faker_num_customers": 1000,
    "faker_num_stock_transactions": 5000,
    "faker_num_crypto_transactions": 3000,
    "news_read_limit": 10,
}

def create_prefect_variables():
    for name, value in VARIABLES.items():
        existing = Variable.get(name, default=None)

        if existing is not None:
            logger.info(
                "skip %s (already exists: %s)",
                name,
                existing,
            )
            continue

        Variable.set(name, value)
        logger.info("set %s=%s", name, value)

if __name__ == "__main__":
    logger.info("starting Prefect variable bootstrap (safe mode)")
    create_prefect_variables()
    logger.info("done")

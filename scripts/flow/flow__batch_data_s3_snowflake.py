from prefect import flow, get_run_logger
from datetime import datetime

# Subflows to orchestrate
from scripts.data_generation.a1_1_raw_data_faker_generator import generate_fake_market_data_flow
from scripts.data_generation.a1_2_news_data_scrapper import crypto_news_scraper
from scripts.data_generation.a1_3_batch_data_to_s3 import data_to_s3_flow
from scripts.data_generation.a1_4_batch_s3_to_snowflake import crypto_news_s3_to_snowflake_flow
from scripts.data_generation.a1_5_batch_s3_to_postgres import batch_s3_to_postgres_flow
from scripts.data_generation.a1_6_batch_dbt_build import run_dbt_build_after_staging
from scripts.utils.date_utils import get_canonical_data_date

@flow(name="flow__batch_data_s3_snowflake")
def batch_data_s3_snowflake(
    run_fake_data: bool = True,
    run_news_scraper: bool = True,
    run_upload_to_s3: bool = True,
    run_s3_to_snowflake: bool = True,
    run_s3_to_postgres: bool = True,
    run_dbt_on_snowflake: bool = True
):
    """
    Orchestrates 4 specified subflows in sequence:

    1) generate_fake_market_data_flow
    2) crypto_news_scraper
    3) data_to_s3_flow
    4) crypto_news_s3_to_snowflake_flow

    Notes:
    - Each step is executed independently with basic error handling so later steps can still run.
    - A single canonical data_date (YYYYMMDD_HHMMSS) is computed once and passed to all subflows for filename consistency.
    """
    logger = get_run_logger()
    logger.info("🚀 Starting batch flow to run 4 subflows")

    results = {
        "fake_market_data": None,
        "news_scraper": None,
        "data_to_s3": None,
        "s3_to_snowflake": None,
        "s3_to_postgres": None,
        "dbt_on_snowflake": None
    }

    # Create a single run data_date suffix to propagate to all subflows
    run_suffix = get_canonical_data_date()

    # 1) Generate fake market data
    if run_fake_data:
        try:
            logger.info("▶️ Running subflow: generate_fake_market_data_flow")
            results["fake_market_data"] = generate_fake_market_data_flow(data_date=run_suffix)
            logger.info("✅ Completed: generate_fake_market_data_flow")
        except Exception as e:
            logger.error(f"❌ Subflow generate_fake_market_data_flow failed: {e}")

    # 2) Scrape crypto news to local CSV
    if run_news_scraper:
        try:
            logger.info("▶️ Running subflow: crypto_news_scraper")
            crypto_news_scraper(data_date=run_suffix)
            results["news_scraper"] = True
            logger.info("✅ Completed: crypto_news_scraper")
        except Exception as e:
            logger.error(f"❌ Subflow crypto_news_scraper failed: {e}")
            results["news_scraper"] = False

    # 3) Upload CSVs to S3/MinIO
    if run_upload_to_s3:
        try:
            logger.info("▶️ Running subflow: data_to_s3_flow")
            results["data_to_s3"] = data_to_s3_flow(data_date=run_suffix)
            logger.info(f"✅ Completed: data_to_s3_flow (result={results['data_to_s3']})")
        except Exception as e:
            logger.error(f"❌ Subflow data_to_s3_flow failed: {e}")
            results["data_to_s3"] = False

    # 4) From S3 to Snowflake staging
    if run_s3_to_snowflake:
        try:
            logger.info("▶️ Running subflow: crypto_news_s3_to_snowflake_flow")
            results["s3_to_snowflake"] = crypto_news_s3_to_snowflake_flow(data_date=run_suffix)
            logger.info(f"✅ Completed: crypto_news_s3_to_snowflake_flow (result={results['s3_to_snowflake']})")
        except Exception as e:
            logger.error(f"❌ Subflow crypto_news_s3_to_snowflake_flow failed: {e}")
            results["s3_to_snowflake"] = False
    # 5) From S3 to Postgres
    if run_s3_to_postgres:
        try:
            logger.info("▶️ Running subflow: batch_s3_to_postgres_flow")
            results["s3_to_postgres"] = batch_s3_to_postgres_flow(data_date=run_suffix)
            logger.info(f"✅ Completed: batch_s3_to_postgres_flow (result={results['s3_to_postgres']})")
        except Exception as e:
            logger.error(f"❌ Subflow batch_s3_to_postgres_flow failed: {e}")
            results["s3_to_postgres"] = False

    # 6) DBT Build for Snowflake
    if run_dbt_on_snowflake:
        try:
            logger.info("▶️ Running subflow: run_dbt_build_after_staging")
            results["dbt_on_snowflake"] = run_dbt_build_after_staging(data_date=run_suffix)
            logger.info(f"✅ Completed: run_dbt_build_after_staging (result={results['s3_to_postgres']})")
        except Exception as e:
            logger.error(f"❌ Subflow run_dbt_build_after_staging failed: {e}")
            results["dbt_on_snowflake"] = False


    logger.info("🏁 Batch flow finished")
    return results


if __name__ == "__main__":
    batch_data_s3_snowflake()

"""
Prefect flow to run dbt validation models via the Trino target.

This flow runs the `validation.compare_raw_counts` dbt model for one or more
table pairs (Postgres raw table vs Snowflake RAW_ table) using the Trino
adapter/target added to `profiles/profiles.yml`.

It assumes `dbt` is installed in the running Python environment and the
current working directory (or project root) contains the dbt project files
(`dbt_project.yml`, `models/`, etc.).
"""

from prefect import flow, get_run_logger
import subprocess
import shlex
import os
from typing import List


@flow(name="DBT Validation")
def run_dbt_validation_flow(tables: List[str] | None = None):
    """Run dbt validation for the provided base table names.

    Each `table` is the base table name in Postgres (e.g., `customers`). The
    corresponding Snowflake raw table is expected to be named `RAW_{TABLE}` in
    the Snowflake DB/SC configured for the `sc_sf` Trino catalog.
    """
    logger = get_run_logger()

    if tables is None:
        tables = ["customers", "transactions"]

    # Project root (two levels up from this file: scripts/dbt -> scripts -> repo)
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

    results = {}
    for table in tables:
        pg_table = f"public.raw_{table}"
        # This uses Trino catalog `sc_sf` + the Snowflake DB/SC from sc_sf.properties
        sf_table = f'sc_sf."DB_T23"."SC_T23".RAW_{table.upper()}'
        
        # Determine key column based on table type
        key_column = "customer_id" if table == "customers" else "transaction_id"
        key_column_upper = key_column.upper()

        cmd = (
            "dbt run --select validation.compare_raw_counts --target trino --vars '"
            + f"{{\"pg_table\": \"{pg_table}\", \"sf_table\": \"{sf_table}\", \"key_column\": \"{key_column}\"}}'"
        )

        logger.info(f"Running dbt validation for table: {table}")
        logger.debug(f"dbt command: {cmd}")

        try:
            proc = subprocess.run(
                shlex.split(cmd),
                cwd=project_root,
                capture_output=True,
                text=True,
                check=False,
            )

            logger.info(f"dbt exit code: {proc.returncode} for {table}")
            if proc.stdout:
                logger.debug(proc.stdout)
            if proc.stderr:
                logger.warning(proc.stderr)

            results[table] = {
                "returncode": proc.returncode,
                "stdout": proc.stdout,
                "stderr": proc.stderr,
            }

        except Exception as e:
            logger.error(f"Failed to run dbt for {table}: {e}")
            results[table] = {"error": str(e)}

    return results


if __name__ == "__main__":
    # quick manual run when executed directly
    print(run_dbt_validation_flow(["customers", "transactions"]))

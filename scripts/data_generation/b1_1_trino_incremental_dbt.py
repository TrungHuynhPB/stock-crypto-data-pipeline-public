"""Prefect flow: run dbt for kafka consumer - run trino selector then snowflake selector.

Context
- Prefect 2.x/3.x compatible
- dbt-trino and dbt-snowflake targets (see `profiles/profiles.yml`: profile `market_data_pipeline`)
- Intended to run after kafka consumer loads data into raw tables (raw_customers, raw_corporates, raw_transactions)

Behavior
1) `dbt run --select raw_customers+ raw_corporates+ raw_transactions+ --selector trino --target dev-trino`
2) `dbt run --select raw_customers+ raw_corporates+ raw_transactions+ --selector snowflake --target dev-snowflake`

The `+` selector includes all downstream models that depend on these 3 raw tables.
The `--selector` filters to only models matching the trino/snowflake selector (intersection).
This ensures Snowflake views are not run with Trino target and vice versa.

Run locally
  uv run --env-file .env python -m scripts.data_generation.b1_1_trino_incremental_dbt
"""

from __future__ import annotations

import os
import shlex
import subprocess
from pathlib import Path

from dotenv import load_dotenv
from prefect import flow, get_run_logger

# Best-effort load .env for local runs. Deployments should inject env vars.
load_dotenv()


def _project_and_profiles_dirs() -> tuple[Path, Path]:
    """Resolve dbt project root and profiles directory."""
    this_file = Path(__file__).resolve()
    project_root = this_file.parents[2]
    profiles_dir = project_root / "profiles"
    return project_root, profiles_dir


def _run_cmd(cmd: str, cwd: Path) -> subprocess.CompletedProcess:
    """Run a command and return the CompletedProcess, logging stdout/stderr."""
    logger = get_run_logger()
    logger.info(f"Running: {cmd} (cwd={cwd})")
    proc = subprocess.run(
        shlex.split(cmd),
        cwd=str(cwd),
        capture_output=True,
        text=True,
        check=False,
        env=os.environ.copy(),
    )
    if proc.stdout:
        logger.info(proc.stdout)
    if proc.returncode != 0 and proc.stderr:
        logger.error(proc.stderr)
    elif proc.stderr:
        # dbt sometimes logs non-fatal warnings to stderr
        logger.warning(proc.stderr)
    return proc


@flow(name="b1_1_trino_incremental_dbt")
def trino_incremental_dbt_flow(
    dbt_profile: str = "market_data_pipeline",
    trino_target: str = "dev-trino",
    snowflake_target: str = "dev-snowflake",
) -> dict:
    """Run dbt for kafka consumer - run trino selector then snowflake selector.

    Only runs models that depend on raw_customers, raw_corporates, and raw_transactions.
    Uses `+` selector to include all downstream models affected by these 3 raw tables.

    Args:
        dbt_profile: dbt profile name in profiles/profiles.yml
        trino_target: target for trino selector (default: dev-trino)
        snowflake_target: target for snowflake selector (default: dev-snowflake)

    Returns:
        dict with dbt run return codes for both targets.
    """
    logger = get_run_logger()
    project_root, profiles_dir = _project_and_profiles_dirs()

    # Select the 3 raw tables and all downstream models
    raw_tables_select = "raw_customers+ raw_corporates+ raw_transaction_corporate+ raw_transaction_personal+"

    # 1) dbt run with trino selector for kafka raw tables and downstream
    # Using --selector trino ensures only trino-tagged models are executed
    logger.info("Running dbt with trino selector for raw_customers, raw_corporates, raw_transaction_coporate raw_transaction_personal and downstream models")
    trino_cmd = (
        f"dbt run --profile {shlex.quote(dbt_profile)} --target {shlex.quote(trino_target)} "
        f"--profiles-dir {shlex.quote(str(profiles_dir))} "
        f"--select {shlex.quote(raw_tables_select)} --selector trino"
    )
    trino_proc = _run_cmd(trino_cmd, cwd=project_root)
    if trino_proc.returncode != 0:
        logger.error("dbt run with trino selector failed")
        return {
            "trino_returncode": trino_proc.returncode,
            "snowflake_returncode": None,
            "success": False,
        }

    # 2) dbt run with snowflake selector for kafka raw tables and downstream
    # Using --selector snowflake ensures only snowflake-tagged models are executed
    logger.info("Running dbt with snowflake selector for raw_customers, raw_corporates, raw_transaction_coporate raw_transaction_personal and downstream models")
    snowflake_cmd = (
        f"dbt run --profile {shlex.quote(dbt_profile)} --target {shlex.quote(snowflake_target)} "
        f"--profiles-dir {shlex.quote(str(profiles_dir))} "
        f"--select {shlex.quote(raw_tables_select)} --selector snowflake"
    )
    snowflake_proc = _run_cmd(snowflake_cmd, cwd=project_root)

    return {
        "trino_returncode": trino_proc.returncode,
        "snowflake_returncode": snowflake_proc.returncode,
        "success": trino_proc.returncode == 0 and snowflake_proc.returncode == 0,
    }


if __name__ == "__main__":
    trino_incremental_dbt_flow()

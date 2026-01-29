"""
Prefect flow to run `dbt build` (Snowflake target) after staging batch CSVs to Snowflake.
- Uses prefect-dbt (DbtCoreOperation) when available
- Falls back to invoking `dbt build` via subprocess if prefect-dbt is not installed

This is intended to be the next step after scripts/data_generation/a1_4_batch_s3_to_snowflake.py
"""

from __future__ import annotations

import os
import shlex
import subprocess
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from prefect import flow, get_run_logger

# Load environment variables from .env if present
load_dotenv()
key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE_PATH")

# Optional import of prefect-dbt. If not available, we'll fallback to subprocess
try:
    from prefect_dbt.cli import DbtCoreOperation  # type: ignore
    _HAS_PREFECT_DBT = True
except Exception:  # pragma: no cover - best-effort fallback
    DbtCoreOperation = None  # type: ignore
    _HAS_PREFECT_DBT = False

# Import the upstream flow that stages data to Snowflake
from scripts.data_generation.a1_4_batch_s3_to_snowflake import (
    crypto_news_s3_to_snowflake_flow,
)


def _project_and_profiles_dirs() -> tuple[Path, Path]:
    """Resolve project root and profiles directory.

    Assumes this file lives at scripts/data_generation/a1_6_*.py
    so project root is two parents up, and profiles dir lives under the root.
    """
    this_file = Path(__file__).resolve()
    project_root = this_file.parents[2]
    profiles_dir = project_root / "profiles"
    return project_root, profiles_dir


def _run_dbt_build_subprocess(target: str, selector: Optional[str]) -> dict:
    """Fallback: run dbt build via subprocess when prefect-dbt is not installed.

    Uses the same invocation style that works locally, e.g.:
        uv run --env-file .env dbt build --profiles-dir <abs_path>/profiles --target <target> --selector <selector>
    """
    logger = get_run_logger()
    project_root, profiles_dir = _project_and_profiles_dirs()

    profiles_dir_str = str(profiles_dir.resolve())
    build_cmd = (
        "dbt build "
        f"--profiles-dir {shlex.quote(profiles_dir_str)} "
        f"--target {shlex.quote(target)}"
    )
    if selector:
        build_cmd += f" --selector {shlex.quote(selector)}"

    logger.warning("prefect-dbt not found. Falling back to subprocess for dbt build.")
    logger.info(f"Running: {build_cmd} (cwd={project_root})")

    try:
        proc = subprocess.run(
            shlex.split(build_cmd),
            cwd=str(project_root),
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.stdout:
            logger.info(proc.stdout)
        if proc.returncode != 0 and proc.stderr:
            logger.error(proc.stderr)
        return {
            "returncode": proc.returncode,
            "stdout": proc.stdout,
            "stderr": proc.stderr,
            "success": proc.returncode == 0,
        }
    except Exception as e:  # pragma: no cover
        logger.error(f"Failed to run dbt build: {e}")
        return {"returncode": -1, "stdout": "", "stderr": str(e), "success": False}


@flow(name="6_batch_dbt_build")
def run_dbt_build_after_staging(
    data_date: Optional[str] = None,
    run_staging_flow: bool = True,
    # When None, we'll run both Snowflake and Trino using sensible defaults.
    dbt_target: Optional[str] = None,
    dbt_selector: Optional[str] = None,
) -> dict:
    """
    Run the Snowflake dbt build after staging S3 CSVs to Snowflake.

    Args:
        data_date: Optional date in YYYYMMDD_HHMMSS to pick specific batch files in S3.
        run_staging_flow: Whether to run the S3->Snowflake staging flow first.
        dbt_target: dbt target name (defaults to env DBT_TARGET or profiles.yml default; commonly 'ci').
        dbt_selector: Optional dbt selector to limit build scope (e.g. 'snowflake' or 'trino').

    Returns:
        A dict-like result from the dbt operation indicating success/return code.
    """
    logger = get_run_logger()

    # 1) Optionally run the upstream staging flow
    staging_ok = True
    if run_staging_flow:
        #logger.info(f"Loaded KEY_PATH: {key_path}")
        logger.info("▶️ Running upstream flow: crypto_news_s3_to_snowflake_flow")
        try:
            staging_ok = bool(crypto_news_s3_to_snowflake_flow(data_date=data_date))
        except Exception as e:
            staging_ok = False
            logger.error(f"Upstream staging flow failed: {e}")

        if not staging_ok:
            logger.error("Staging failed. Proceeding to dbt build anyway (best-effort).")

    # 2) Decide which targets/selectors to run.
    # If explicit values are provided, run only that combination.
    # Otherwise, default to running BOTH Trino and Snowflake builds, in that order:
    #   - Trino:     target dev-trino,     selector trino
    #   - Snowflake: target dev-snowflake, selector snowflake
    runs: list[dict] = []

    if dbt_target and dbt_selector:
        run_plan = [(dbt_target, dbt_selector)]
    else:
        # Default: run Trino first, then Snowflake
        run_plan = [
            ("dev-trino", "trino"),
            ("dev-snowflake", "snowflake"),
        ]

    for target, selector in run_plan:
        logger.info(f"🚀 Starting dbt build (target={target}, selector={selector})")

        # Always use the subprocess path so the behavior exactly matches your
        # working local commands (`uv run --env-file .env dbt build ...`).
        subprocess_result = _run_dbt_build_subprocess(target=target, selector=selector)
        runs.append(
            {
                "target": target,
                "selector": selector,
                **subprocess_result,
            }
        )

    overall_success = all(r.get("success", False) for r in runs)
    return {
        "staging_ok": staging_ok,
        "overall_success": overall_success,
        "runs": runs,
    }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Run dbt build after S3->Snowflake staging. "
        "By default, runs both dev-snowflake (selector snowflake) and dev-trino (selector trino). "
        "Provide --target and --selector together to run only a single combination."
    )
    parser.add_argument("--data-date", dest="data_date", help="YYYYMMDD_HHMMSS date to select batch files", default=None)
    parser.add_argument(
        "--skip-staging", dest="skip_staging", action="store_true", help="Skip running the staging flow"
    )
    parser.add_argument("--target", dest="dbt_target", help="dbt target (default: env/ci)", default=None)
    parser.add_argument(
        "--selector",
        dest="dbt_selector",
        help="dbt selector (e.g. 'snowflake' or 'trino', optional)",
        default=None,
    )

    args = parser.parse_args()

    run_dbt_build_after_staging(
        data_date=args.data_date,
        run_staging_flow=not args.skip_staging,
        dbt_target=args.dbt_target,
        dbt_selector=args.dbt_selector,
    )

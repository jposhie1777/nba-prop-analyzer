"""
run_bq_pipeline.py

Executes SQL files in order for a given league pipeline.
Called by GitHub Actions workflow.

Usage:
  python scripts/run_bq_pipeline.py --league mls
  python scripts/run_bq_pipeline.py --league epl
  python scripts/run_bq_pipeline.py --league all
"""

import argparse
import os
import sys
from pathlib import Path
from google.cloud import bigquery

PROJECT_ID = "graphite-flare-477419-h7"

# ── Pipeline definitions ──────────────────────────────────────────────────────
# Add new leagues/steps here as the platform grows.
# Files are executed in list order — order matters.
PIPELINES = {
    "mls": [
        "sql/mls/01_mls_team_name_map.sql",
        "sql/mls/02_mls_team_form.sql",
        "sql/mls/03_mls_betting_analytics.sql",
    ],
    "epl": [
        # EPL files will go here when EPL data is healthy
        # "sql/epl/01_epl_team_form.sql",
        # "sql/epl/02_epl_betting_analytics.sql",
    ],
}


def run_sql_file(client: bigquery.Client, sql_path: str) -> None:
    """Read and execute a SQL file against BigQuery."""
    path = Path(sql_path)

    if not path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    sql = path.read_text()

    print(f"\n{'─' * 60}")
    print(f"▶  Running: {sql_path}")
    print(f"{'─' * 60}")

    job = client.query(sql)
    job.result()  # Wait for completion

    print(f"✅ Done: {sql_path}")
    if job.num_dml_affected_rows is not None:
        print(f"   Rows affected: {job.num_dml_affected_rows:,}")


def run_pipeline(league: str) -> None:
    """Run all SQL steps for a given league."""
    client = bigquery.Client(project=PROJECT_ID)

    if league == "all":
        leagues = list(PIPELINES.keys())
    else:
        leagues = [league]

    for lg in leagues:
        steps = PIPELINES.get(lg, [])

        if not steps:
            print(f"\n⚠️  No SQL steps defined for league: {lg} — skipping")
            continue

        print(f"\n{'═' * 60}")
        print(f"  🏟️  Starting pipeline: {lg.upper()}")
        print(f"{'═' * 60}")

        for sql_file in steps:
            try:
                run_sql_file(client, sql_file)
            except Exception as e:
                print(f"\n❌ FAILED: {sql_file}")
                print(f"   Error: {e}")
                sys.exit(1)

        print(f"\n🎉 Pipeline complete: {lg.upper()}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run BQ analytics pipeline")
    parser.add_argument(
        "--league",
        required=True,
        choices=["mls", "epl", "all"],
        help="Which league pipeline to run"
    )
    args = parser.parse_args()
    run_pipeline(args.league)

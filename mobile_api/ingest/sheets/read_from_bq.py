"""Read today's matches from the BigQuery sheet_daily_matches table.

Returns the data as a pandas DataFrame for downstream use (APIs, models, etc.).

Usage (standalone):
    python mobile_api/ingest/sheets/read_from_bq.py

    # Read a specific date
    SHEETS_MATCH_DATE=2026-02-13 python mobile_api/ingest/sheets/read_from_bq.py

Environment variables:
    GOOGLE_APPLICATION_CREDENTIALS_JSON  Service-account JSON (string)
    GOOGLE_APPLICATION_CREDENTIALS       Path to service-account JSON file
    GCP_PROJECT                          Google Cloud project ID
    SHEETS_BQ_DATASET                    BigQuery dataset (default: atp_data)
    SHEETS_BQ_TABLE                      BigQuery table  (default: atp_data.sheet_daily_matches)
    SHEETS_MATCH_DATE                    Date to query (YYYY-MM-DD); defaults to today EST
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

EST = ZoneInfo("America/New_York")
from pathlib import Path
from typing import Optional

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# ======================================================
# Config
# ======================================================

DEFAULT_DATASET = os.getenv("SHEETS_BQ_DATASET", "atp_data")
DEFAULT_TABLE = os.getenv("SHEETS_BQ_TABLE", f"{DEFAULT_DATASET}.sheet_daily_matches")

SCOPES = [
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/bigquery",
]


# ======================================================
# Auth
# ======================================================


def _get_credentials() -> service_account.Credentials:
    creds_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if creds_json:
        info = json.loads(creds_json)
        return service_account.Credentials.from_service_account_info(info, scopes=SCOPES)

    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if creds_path:
        return service_account.Credentials.from_service_account_file(creds_path, scopes=SCOPES)

    raise RuntimeError(
        "Set GOOGLE_APPLICATION_CREDENTIALS_JSON or GOOGLE_APPLICATION_CREDENTIALS"
    )


def get_bq_client() -> bigquery.Client:
    credentials = _get_credentials()
    project = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
    if project:
        return bigquery.Client(project=project, credentials=credentials)
    return bigquery.Client(credentials=credentials)


# ======================================================
# Query helpers
# ======================================================


def resolve_table_id(table: str, project: str) -> str:
    parts = table.split(".")
    if len(parts) == 3:
        return table
    if len(parts) == 2:
        return f"{project}.{table}"
    return f"{project}.{DEFAULT_DATASET}.{table}"


def read_daily_matches(
    *,
    match_date: Optional[str] = None,
    table: str = DEFAULT_TABLE,
) -> pd.DataFrame:
    """Query today's matches from BigQuery and return as a DataFrame."""
    client = get_bq_client()
    table_id = resolve_table_id(table, client.project)

    if match_date is None:
        match_date = datetime.now(EST).strftime("%Y-%m-%d")

    query = f"""
        SELECT *
        FROM `{table_id}`
        WHERE match_date = @match_date
        ORDER BY scheduled_time ASC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("match_date", "DATE", match_date),
        ]
    )

    print(f"  Querying {table_id} for match_date = {match_date} ...")
    df = client.query(query, job_config=job_config).to_dataframe()
    print(f"  Returned {len(df)} rows")
    return df


def read_all_matches(
    *,
    table: str = DEFAULT_TABLE,
    limit: int = 1000,
) -> pd.DataFrame:
    """Query all matches from the table (with a safety limit)."""
    client = get_bq_client()
    table_id = resolve_table_id(table, client.project)

    query = f"""
        SELECT *
        FROM `{table_id}`
        ORDER BY match_date DESC, scheduled_time ASC
        LIMIT @limit
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
        ]
    )

    print(f"  Querying {table_id} (limit {limit}) ...")
    df = client.query(query, job_config=job_config).to_dataframe()
    print(f"  Returned {len(df)} rows")
    return df


# ======================================================
# CLI
# ======================================================


def main() -> None:
    match_date = os.getenv("SHEETS_MATCH_DATE")

    print("=" * 60)
    print("BigQuery Sheet Matches Reader")
    print(f"  Table      : {DEFAULT_TABLE}")
    print(f"  Match date : {match_date or 'today (EST)'}")
    print("=" * 60)

    df = read_daily_matches(match_date=match_date)

    if df.empty:
        print("\nNo matches found.")
        return

    # Display summary
    print(f"\n{'='*60}")
    print(f"Found {len(df)} match(es):\n")

    display_cols = [
        "match_id", "tournament_name", "round",
        "player1_full_name", "player2_full_name",
        "score", "match_status", "scheduled_time",
    ]
    available_cols = [c for c in display_cols if c in df.columns]
    print(df[available_cols].to_string(index=False))

    # Also output as JSON for piping
    print(f"\n{'='*60}")
    print("JSON output:")
    print(df.to_json(orient="records", date_format="iso", indent=2))


if __name__ == "__main__":
    main()

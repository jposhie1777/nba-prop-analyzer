"""ATP backfill script — mirrors the PGA backfill pattern.

Usage (GitHub Action or local):
    python mobile_api/ingest/atp/backfill.py

Environment variables:
    ATP_DATASET           BigQuery dataset (default: atp_data)
    ATP_BQ_LOCATION       BigQuery location (default: US)
    ATP_BACKFILL_YEARS    How many seasons back to fetch (default: 5)
    ATP_START_SEASON      Explicit start season (overrides BACKFILL_YEARS)
    ATP_END_SEASON        Explicit end season (overrides BACKFILL_YEARS)
    ATP_TRUNCATE          Truncate tables before loading (default: false)
    BDL_ATP_API_KEY       BallDontLie ATP API key
    BDL_ATP_TIER          Rate-limit tier: ALL_STAR or GOAT (default: ALL_STAR)
    GCP_PROJECT           Google Cloud project ID
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime
from pathlib import Path

# Allow running as: python mobile_api/ingest/atp/backfill.py
REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from google.cloud import bigquery

from mobile_api.ingest.atp.ingest import (
    DEFAULT_DATASET,
    DEFAULT_LOCATION,
    ATP_MATCHES_TABLE,
    ATP_PLAYERS_TABLE,
    ATP_RACE_TABLE,
    ATP_RANKINGS_TABLE,
    ATP_TOURNAMENTS_TABLE,
    get_bq_client,
    ingest_historical,
    resolve_table_id,
)


def _season_range() -> tuple[int, int]:
    start = os.getenv("ATP_START_SEASON")
    end = os.getenv("ATP_END_SEASON")
    if start and end:
        return int(start), int(end)
    years_back = int(os.getenv("ATP_BACKFILL_YEARS", "5"))
    current = datetime.utcnow().year
    return current - years_back + 1, current


def _truncate_table(client: bigquery.Client, table: str) -> None:
    table_id = resolve_table_id(table, client.project)
    print(f"  Truncating {table_id} ...")
    client.query(f"TRUNCATE TABLE `{table_id}`").result()


def main() -> None:
    start_season, end_season = _season_range()
    do_truncate = os.getenv("ATP_TRUNCATE", "false").lower() == "true"

    print("=" * 60)
    print("ATP Backfill")
    print(f"  Seasons      : {start_season} – {end_season}")
    print(f"  Truncate     : {do_truncate}")
    print(f"  Dataset      : {DEFAULT_DATASET}")
    print(f"  Tier         : {os.getenv('BDL_ATP_TIER', 'ALL_STAR')}")
    print("=" * 60)

    if do_truncate:
        print("\nTruncating existing tables ...")
        client = get_bq_client()
        for table in [
            ATP_PLAYERS_TABLE,
            ATP_TOURNAMENTS_TABLE,
            ATP_MATCHES_TABLE,
            ATP_RANKINGS_TABLE,
            ATP_RACE_TABLE,
        ]:
            try:
                _truncate_table(client, table)
            except Exception as exc:
                # Table might not exist yet on first run
                print(f"  (skipped — {exc})")

    print(f"\nStarting historical ingest for seasons {start_season}–{end_season} ...\n")
    result = ingest_historical(
        start_season=start_season,
        end_season=end_season,
        include_players=True,
        include_tournaments=True,
        include_matches=True,
        include_rankings=True,
        include_atp_race=True,
        create_tables=True,
    )

    print("\n" + "=" * 60)
    print("ATP Backfill complete.")
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()

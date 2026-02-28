"""
Ingest PGA Tour priority rankings (FedEx Cup standings, etc.) → BigQuery.

Fetches ranked player lists for all categories for a tour and season, then
writes one row per (player, category) to the ``priority_rankings`` BigQuery table.

Usage (standalone CLI):
    python -m mobile_api.ingest.pga.pga_rankings_ingest --year 2025 --tour R
    python -m mobile_api.ingest.pga.pga_rankings_ingest --year 2025 --dry-run
"""

from __future__ import annotations

import argparse
import datetime
import os
import time
from typing import Optional

from google.cloud import bigquery

from .pga_rankings_scraper import fetch_priority_rankings, rankings_to_records

DATASET = os.getenv("PGA_DATASET", "pga_data")
TABLE = os.getenv("PGA_RANKINGS_TABLE", "priority_rankings")
CHUNK_SIZE = 500

_SCHEMA = [
    bigquery.SchemaField("run_ts", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("tour_code", "STRING"),
    bigquery.SchemaField("year", "INTEGER"),
    bigquery.SchemaField("display_year", "STRING"),
    bigquery.SchemaField("through_text", "STRING"),
    bigquery.SchemaField("category_name", "STRING"),
    bigquery.SchemaField("rank", "INTEGER"),
    bigquery.SchemaField("player_id", "STRING"),
    bigquery.SchemaField("display_name", "STRING"),
]


def _bq_client() -> bigquery.Client:
    project = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
    return bigquery.Client(project=project)


def _ensure_table(client: bigquery.Client) -> str:
    """Create the priority_rankings table if it doesn't exist; return the full table ID."""
    bq_table = bigquery.Table(
        f"{client.project}.{DATASET}.{TABLE}",
        schema=_SCHEMA,
    )
    bq_table.range_partitioning = bigquery.RangePartitioning(
        field="year",
        range_=bigquery.PartitionRange(start=2015, end=2035, interval=1),
    )
    bq_table.clustering_fields = ["tour_code", "year", "category_name", "player_id"]
    bq_table.description = (
        "PGA Tour priority rankings / FedEx Cup standings (priorityRankings GraphQL)"
    )
    client.create_table(bq_table, exists_ok=True)
    return f"{client.project}.{DATASET}.{TABLE}"


def ingest_rankings(
    year: int,
    tour_code: str = "R",
    dry_run: bool = False,
    create_tables: bool = True,
    run_ts: Optional[str] = None,
) -> dict:
    """
    Fetch priority rankings from the PGA Tour GraphQL API and write to BigQuery.

    Args:
        year:          Season year.
        tour_code:     Tour code, e.g. ``"R"`` (PGA Tour).
        dry_run:       If True, fetch data but skip BigQuery writes.
        create_tables: Auto-create the BigQuery table if it doesn't exist.
        run_ts:        ISO-8601 timestamp string for run_ts / ingested_at fields.

    Returns:
        Dict with ``rows_fetched`` and ``rows_inserted`` counts.
    """
    ts = run_ts or datetime.datetime.utcnow().isoformat()

    result = fetch_priority_rankings(tour_code=tour_code, year=year)
    records = rankings_to_records(result, run_ts=ts)

    rows_fetched = len(records)
    print(f"[rankings] Fetched {rows_fetched} ranked-player rows for {tour_code}/{year}.")

    if dry_run or not records:
        return {"rows_fetched": rows_fetched, "rows_inserted": 0}

    client = _bq_client()
    if create_tables:
        table_id = _ensure_table(client)
    else:
        table_id = f"{client.project}.{DATASET}.{TABLE}"

    inserted = 0
    for i in range(0, len(records), CHUNK_SIZE):
        chunk = records[i : i + CHUNK_SIZE]
        errors = client.insert_rows_json(table_id, chunk)
        if errors:
            raise RuntimeError(f"BigQuery insert errors: {errors}")
        inserted += len(chunk)
        time.sleep(0.05)

    print(f"[rankings] Inserted {inserted} rows into {table_id}.")
    return {"rows_fetched": rows_fetched, "rows_inserted": inserted}


def _cli() -> None:
    parser = argparse.ArgumentParser(description="Ingest PGA priority rankings to BigQuery.")
    parser.add_argument("--year", type=int, default=datetime.datetime.utcnow().year)
    parser.add_argument("--tour", default="R", metavar="TOUR_CODE")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-create-table", action="store_true")
    args = parser.parse_args()
    result = ingest_rankings(
        year=args.year,
        tour_code=args.tour,
        dry_run=args.dry_run,
        create_tables=not args.no_create_table,
    )
    print(result)


if __name__ == "__main__":
    _cli()

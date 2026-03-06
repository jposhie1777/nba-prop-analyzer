"""
Ingest PGA Tour player stats (profile stats endpoint) → BigQuery.

Fetches all stat leaderboards for a tour and season, then writes one row
per (player, stat) to the ``player_stats`` BigQuery table.

Usage (standalone CLI):
    python -m mobile_api.ingest.pga.pga_stats_ingest --year 2025 --tour R
    python -m mobile_api.ingest.pga.pga_stats_ingest --year 2025 --dry-run
"""

from __future__ import annotations

import argparse
import datetime
import os
import time
from typing import Any, Dict, List, Optional, Set, Tuple

from google.api_core.exceptions import BadRequest
from google.cloud import bigquery

from .pga_stats_scraper import fetch_stat_overview, stat_players_to_records

DATASET = os.getenv("PGA_DATASET", "pga_data")
TABLE = os.getenv("PGA_STATS_TABLE", "player_stats")
WEBSITE_TABLE = os.getenv("PGA_WEBSITE_PLAYER_STATS_TABLE", "website_player_stats")
CHUNK_SIZE = 500
PLAYERS_TABLE = os.getenv("PGA_PLAYERS_TABLE", "players_active")

_SCHEMA = [
    bigquery.SchemaField("run_ts", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("tour_code", "STRING"),
    bigquery.SchemaField("year", "INTEGER"),
    bigquery.SchemaField("stat_id", "STRING"),
    bigquery.SchemaField("stat_name", "STRING"),
    bigquery.SchemaField("tour_avg", "STRING"),
    bigquery.SchemaField("player_id", "STRING"),
    bigquery.SchemaField("player_name", "STRING"),
    bigquery.SchemaField("stat_title", "STRING"),
    bigquery.SchemaField("stat_value", "STRING"),
    bigquery.SchemaField("rank", "INTEGER"),
    bigquery.SchemaField("country", "STRING"),
    bigquery.SchemaField("country_flag", "STRING"),
]

_WEBSITE_SCHEMA = [
    bigquery.SchemaField("run_ts", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("tour_code", "STRING"),
    bigquery.SchemaField("season_year", "INTEGER"),
    bigquery.SchemaField("player_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("player_name", "STRING"),
    bigquery.SchemaField("country", "STRING"),
    bigquery.SchemaField("country_flag", "STRING"),
    bigquery.SchemaField("stat_id", "STRING"),
    bigquery.SchemaField("stat_name", "STRING"),
    bigquery.SchemaField("stat_title", "STRING"),
    bigquery.SchemaField("stat_value", "STRING"),
    bigquery.SchemaField("rank", "INTEGER"),
    bigquery.SchemaField("tour_avg", "STRING"),
]



def _bq_client() -> bigquery.Client:
    project = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
    return bigquery.Client(project=project)


def _fetch_active_player_ids(client: bigquery.Client) -> List[str]:
    table_id = f"`{client.project}.{DATASET}.{PLAYERS_TABLE}`"
    query = f"""
    WITH latest AS (
      SELECT * EXCEPT(row_num)
      FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY run_ts DESC) AS row_num
        FROM {table_id}
      )
      WHERE row_num = 1
    )
    SELECT CAST(player_id AS STRING) AS player_id
    FROM latest
    WHERE COALESCE(active, TRUE) = TRUE
    ORDER BY player_id
    """
    rows = client.query(query).result()
    return [str(row.get("player_id")) for row in rows if row.get("player_id") is not None]


def _ensure_table(client: bigquery.Client) -> str:
    """Create the player_stats table if it doesn't exist; return the full table ID."""
    bq_table = bigquery.Table(
        f"{client.project}.{DATASET}.{TABLE}",
        schema=_SCHEMA,
    )
    bq_table.range_partitioning = bigquery.RangePartitioning(
        field="year",
        range_=bigquery.PartitionRange(start=2015, end=2035, interval=1),
    )
    bq_table.clustering_fields = ["tour_code", "stat_id", "player_id"]
    bq_table.description = "PGA Tour per-player stat rows from profile stats endpoint"
    client.create_table(bq_table, exists_ok=True)
    return f"{client.project}.{DATASET}.{TABLE}"


def _ensure_website_table(client: bigquery.Client) -> str:
    """Create flattened website stats table if needed."""
    bq_table = bigquery.Table(
        f"{client.project}.{DATASET}.{WEBSITE_TABLE}",
        schema=_WEBSITE_SCHEMA,
    )
    bq_table.range_partitioning = bigquery.RangePartitioning(
        field="season_year",
        range_=bigquery.PartitionRange(start=2015, end=2035, interval=1),
    )
    bq_table.clustering_fields = ["tour_code", "season_year", "player_id"]
    bq_table.description = "PGA website player stats flattened (one row per player/stat)"
    client.create_table(bq_table, exists_ok=True)
    return f"{client.project}.{DATASET}.{WEBSITE_TABLE}"


def ingest_website_player_stats(
    year: int,
    tour_code: str = "R",
    dry_run: bool = False,
    create_tables: bool = True,
    run_ts: Optional[str] = None,
) -> dict:
    """Ingest flattened one-row-per-player-stat data into website_player_stats."""
    ts = run_ts or datetime.datetime.utcnow().isoformat()
    client = _bq_client()
    player_ids = _fetch_active_player_ids(client)
    if not player_ids:
        print("[website_player_stats] No active PGA players found; skipping.")
        return {"rows_fetched": 0, "rows_inserted": 0, "active_players": 0}

    result = fetch_stat_overview(tour_code=tour_code, year=year, player_ids=player_ids)
    per_stat_rows = stat_players_to_records(result, run_ts=ts)
    website_rows: List[Dict[str, Any]] = []
    for row in per_stat_rows:
        mapped = dict(row)
        mapped["season_year"] = mapped.pop("year", year)
        website_rows.append(mapped)

    rows_fetched = len(website_rows)
    print(f"[website_player_stats] Built {rows_fetched} player-stat rows for {tour_code}/{year}.")

    if create_tables:
        table_id = _ensure_website_table(client)
    else:
        table_id = f"{client.project}.{DATASET}.{WEBSITE_TABLE}"

    if dry_run or not website_rows:
        return {"rows_fetched": rows_fetched, "rows_inserted": 0, "active_players": len(player_ids)}

    delete_sql = f"DELETE FROM `{table_id}` WHERE tour_code = @tour_code AND season_year = @season_year"
    delete_cfg = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("tour_code", "STRING", tour_code),
            bigquery.ScalarQueryParameter("season_year", "INT64", int(year)),
        ]
    )
    client.query(delete_sql, job_config=delete_cfg).result()

    inserted = 0
    for i in range(0, len(website_rows), CHUNK_SIZE):
        chunk = website_rows[i : i + CHUNK_SIZE]
        errors = client.insert_rows_json(table_id, chunk)
        if errors:
            raise RuntimeError(f"BigQuery insert errors: {errors}")
        inserted += len(chunk)
        time.sleep(0.05)

    return {"rows_fetched": rows_fetched, "rows_inserted": inserted, "active_players": len(player_ids)}


def ingest_stats(
    year: int,
    tour_code: str = "R",
    dry_run: bool = False,
    create_tables: bool = True,
    run_ts: Optional[str] = None,
) -> dict:
    """
    Fetch stats from PGA player profile stats API and write to BigQuery.

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
    client = _bq_client()
    player_ids = _fetch_active_player_ids(client)
    if not player_ids:
        print("[stats] No active PGA players found; skipping stats ingest.")
        return {"rows_fetched": 0, "rows_inserted": 0, "active_players": 0}

    print(f"[stats] Fetching stats for {len(player_ids)} active players ({tour_code}/{year}).")
    result = fetch_stat_overview(tour_code=tour_code, year=year, player_ids=player_ids)
    records = stat_players_to_records(result, run_ts=ts)

    rows_fetched = len(records)
    print(f"[stats] Fetched {rows_fetched} stat-player rows for {tour_code}/{year}.")

    if dry_run or not records:
        return {"rows_fetched": rows_fetched, "rows_inserted": 0, "active_players": len(player_ids)}

    if create_tables:
        table_id = _ensure_table(client)
    else:
        table_id = f"{client.project}.{DATASET}.{TABLE}"

    # Keep one fresh snapshot per (tour_code, year) and avoid duplicate accumulation.
    delete_sql = f"DELETE FROM `{table_id}` WHERE tour_code = @tour_code AND year = @year"
    query_params = [
        bigquery.ScalarQueryParameter("tour_code", "STRING", tour_code),
        bigquery.ScalarQueryParameter("year", "INT64", int(year)),
    ]
    delete_cfg = bigquery.QueryJobConfig(query_parameters=query_params)

    delete_performed = True
    try:
        client.query(delete_sql, job_config=delete_cfg).result()
    except BadRequest as exc:
        message = str(exc)
        if "streaming buffer" in message and "not supported" in message:
            delete_performed = False
            print(
                "[stats] Delete skipped because table has a streaming buffer; "
                "falling back to insert-time de-duplication."
            )
        else:
            raise

    if not delete_performed:
        existing_sql = f"""
            SELECT stat_id, player_id, rank, stat_value
            FROM `{table_id}`
            WHERE tour_code = @tour_code AND year = @year
        """
        existing_cfg = bigquery.QueryJobConfig(query_parameters=query_params)
        existing_rows = client.query(existing_sql, job_config=existing_cfg).result()
        existing_keys: Set[Tuple[str, str, int, str]] = {
            (
                str(row.get("stat_id") or ""),
                str(row.get("player_id") or ""),
                int(row.get("rank") or 0),
                str(row.get("stat_value") or ""),
            )
            for row in existing_rows
        }

        deduped_records: List[Dict[str, object]] = []
        for record in records:
            key = (
                str(record.get("stat_id") or ""),
                str(record.get("player_id") or ""),
                int(record.get("rank") or 0),
                str(record.get("stat_value") or ""),
            )
            if key in existing_keys:
                continue
            deduped_records.append(record)
        records = deduped_records

    inserted = 0
    for i in range(0, len(records), CHUNK_SIZE):
        chunk = records[i : i + CHUNK_SIZE]
        errors = client.insert_rows_json(table_id, chunk)
        if errors:
            raise RuntimeError(f"BigQuery insert errors: {errors}")
        inserted += len(chunk)
        time.sleep(0.05)

    print(f"[stats] Inserted {inserted} rows into {table_id}.")
    return {"rows_fetched": rows_fetched, "rows_inserted": inserted, "active_players": len(player_ids)}


def _cli() -> None:
    parser = argparse.ArgumentParser(description="Ingest PGA player stats to BigQuery.")
    parser.add_argument("--year", type=int, default=datetime.datetime.utcnow().year)
    parser.add_argument("--tour", default="R", metavar="TOUR_CODE")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-create-table", action="store_true")
    args = parser.parse_args()
    result = ingest_stats(
        year=args.year,
        tour_code=args.tour,
        dry_run=args.dry_run,
        create_tables=not args.no_create_table,
    )
    print(result)


if __name__ == "__main__":
    _cli()

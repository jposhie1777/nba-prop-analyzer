"""
Ingest PGA Tour round pairings (tee times + groups) into BigQuery.

Reads from the official PGA Tour GraphQL endpoint and writes to
``pga_data.tournament_round_pairings`` (or a table configured via env vars).
"""

from __future__ import annotations

import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from google.api_core.exceptions import Conflict, NotFound
from google.cloud import bigquery

from bq import get_bq_client

from .pga_tour_graphql import fetch_pairings, pairings_to_records

DATASET = os.getenv("PGA_DATASET", "pga_data")
PAIRINGS_TABLE = os.getenv("PGA_PAIRINGS_TABLE", "tournament_round_pairings")
PAIRINGS_VIEW = os.getenv("PGA_PAIRINGS_VIEW", "v_pairings_latest")
DATASET_LOCATION = os.getenv("PGA_DATASET_LOCATION", "US")

SCHEMA_PAIRINGS = [
    bigquery.SchemaField("run_ts", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("tournament_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("round_number", "INT64"),
    bigquery.SchemaField("round_status", "STRING"),
    bigquery.SchemaField("group_number", "INT64"),
    bigquery.SchemaField("tee_time", "STRING"),
    bigquery.SchemaField("start_hole", "INT64"),
    bigquery.SchemaField("back_nine", "BOOL"),
    bigquery.SchemaField("course_id", "STRING"),
    bigquery.SchemaField("course_name", "STRING"),
    bigquery.SchemaField("player_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("player_display_name", "STRING"),
    bigquery.SchemaField("player_first_name", "STRING"),
    bigquery.SchemaField("player_last_name", "STRING"),
    bigquery.SchemaField("country", "STRING"),
    bigquery.SchemaField("world_rank", "INT64"),
    bigquery.SchemaField("amateur", "BOOL"),
]


def _resolve_table_id(table: str, project: str) -> str:
    parts = table.split(".")
    if len(parts) == 3:
        return table
    if len(parts) == 2:
        return f"{project}.{table}"
    return f"{project}.{DATASET}.{table}"


def _dataset_id(table_id: str) -> str:
    parts = table_id.split(".")
    if len(parts) < 2:
        raise ValueError(f"Invalid table id: {table_id}")
    return ".".join(parts[:2])


def ensure_dataset(client: bigquery.Client, dataset_id: str) -> None:
    try:
        client.get_dataset(dataset_id)
    except NotFound:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = DATASET_LOCATION
        client.create_dataset(dataset)
    except Conflict:
        return


def ensure_table(client: bigquery.Client, table_id: str) -> None:
    try:
        client.get_table(table_id)
    except NotFound:
        table = bigquery.Table(table_id, schema=SCHEMA_PAIRINGS)
        # Partition by round_number for efficient per-round queries.
        table.range_partitioning = bigquery.RangePartitioning(
            field="round_number",
            range_=bigquery.PartitionRange(start=1, end=10, interval=1),
        )
        table.clustering_fields = ["tournament_id", "group_number"]
        client.create_table(table)
    except Conflict:
        return


def ensure_pairings_view(client: bigquery.Client, raw_table_id: str) -> None:
    """Create or replace the v_pairings_latest view in BigQuery.

    The view exposes a deduplicated snapshot: one row per
    (tournament_id, round_number, group_number, player_id) using the
    most-recent ``run_ts`` across all ingest runs.
    """
    view_id = _resolve_table_id(PAIRINGS_VIEW, client.project)
    view_query = f"""
    SELECT * EXCEPT (row_num)
    FROM (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY tournament_id, round_number, group_number, player_id
          ORDER BY run_ts DESC
        ) AS row_num
      FROM `{raw_table_id}`
    )
    WHERE row_num = 1
    """
    view = bigquery.Table(view_id)
    view.view_query = view_query
    try:
        client.get_table(view_id)
        client.update_table(view, ["view_query"])
    except NotFound:
        client.create_table(view)


def insert_rows(
    client: bigquery.Client,
    table: str,
    rows: List[Dict[str, Any]],
    *,
    batch_size: int = 500,
) -> int:
    if not rows:
        return 0
    table_id = _resolve_table_id(table, client.project)
    total = 0
    for idx in range(0, len(rows), batch_size):
        batch = rows[idx : idx + batch_size]
        errors = client.insert_rows_json(table_id, batch)
        if errors:
            raise RuntimeError(f"BigQuery insert errors: {errors[:3]}")
        total += len(batch)
        time.sleep(0.05)
    return total


def ingest_pairings(
    tournament_id: str,
    round_number: int,
    *,
    cut: Optional[str] = None,
    create_tables: bool = True,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Fetch pairings from PGA Tour GraphQL and insert into BigQuery.

    Args:
        tournament_id:  PGA Tour tournament ID, e.g. ``"R2025016"``.
        round_number:   Round to fetch (1–4).
        cut:            Optional cut filter: ``"ALL"``, ``"MADE"``, ``"MISSED"``.
        create_tables:  Auto-create the BigQuery dataset/table if missing.
        dry_run:        If ``True``, fetch data but skip the BigQuery insert.

    Returns:
        Summary dict with record counts and status.
    """
    run_ts = datetime.utcnow().isoformat()

    pairings = fetch_pairings(tournament_id, str(round_number))
    rows = pairings_to_records(pairings, run_ts=run_ts)

    summary: Dict[str, Any] = {
        "tournament_id": tournament_id,
        "round_number": round_number,
        "groups": len(pairings),
        "player_rows": len(rows),
        "dry_run": dry_run,
    }

    if dry_run:
        summary["inserted"] = 0
        return summary

    client = get_bq_client()
    table_id = _resolve_table_id(PAIRINGS_TABLE, client.project)
    if create_tables:
        ensure_dataset(client, _dataset_id(table_id))
        ensure_table(client, table_id)
        ensure_pairings_view(client, table_id)

    summary["inserted"] = insert_rows(client, PAIRINGS_TABLE, rows)
    return summary

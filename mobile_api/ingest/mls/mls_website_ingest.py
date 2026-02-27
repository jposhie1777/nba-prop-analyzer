"""
BigQuery ingest layer for mlssoccer.com scraped data.

Writes three datasets to BigQuery:
  - mls_data.mlssoccer_schedule     (match schedule from stats-api.mlssoccer.com)
  - mls_data.mlssoccer_team_stats   (per-club season stats)
  - mls_data.mlssoccer_player_stats (per-player season stats)

Usage
-----
    from mobile_api.ingest.mls.mls_website_ingest import run_website_ingestion

    result = run_website_ingestion(season=2025)
    print(result)
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

try:
    from .mlssoccer_scraper import fetch_schedule, fetch_team_stats, fetch_player_stats
except ImportError:
    from mobile_api.ingest.mls.mlssoccer_scraper import fetch_schedule, fetch_team_stats, fetch_player_stats

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DATASET = os.getenv("MLS_DATASET", "mls_data")
LOCATION = os.getenv("MLS_BQ_LOCATION", "US")

TABLE_SCHEDULE = os.getenv("MLS_SCHEDULE_TABLE", f"{DATASET}.mlssoccer_schedule")
TABLE_TEAM_STATS = os.getenv("MLS_TEAM_STATS_TABLE", f"{DATASET}.mlssoccer_team_stats")
TABLE_PLAYER_STATS = os.getenv("MLS_PLAYER_STATS_TABLE", f"{DATASET}.mlssoccer_player_stats")


# ---------------------------------------------------------------------------
# BigQuery helpers (shared pattern with ingest.py)
# ---------------------------------------------------------------------------

def _get_bq_client() -> bigquery.Client:
    project = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
    return bigquery.Client(project=project) if project else bigquery.Client()


def _table_id(client: bigquery.Client, table: str) -> str:
    if table.count(".") == 2:
        return table
    return f"{client.project}.{table}"


def _ensure_dataset(client: bigquery.Client, table: str) -> None:
    table_id = _table_id(client, table)
    parts = table_id.split(".")
    dataset_id = ".".join(parts[:2])
    try:
        client.get_dataset(dataset_id)
    except NotFound:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = LOCATION
        client.create_dataset(dataset)
        logger.info("Created dataset %s", dataset_id)


def _ensure_table(client: bigquery.Client, table: str) -> str:
    table_id = _table_id(client, table)
    try:
        client.get_table(table_id)
    except NotFound:
        schema = [
            bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("season", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("entity_id", "STRING"),
            bigquery.SchemaField("payload", "STRING"),
        ]
        bq_table = bigquery.Table(table_id, schema=schema)
        bq_table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="ingested_at",
        )
        bq_table.clustering_fields = ["season", "entity_id"]
        client.create_table(bq_table)
        logger.info("Created table %s", table_id)
    return table_id


def _write_rows(
    client: bigquery.Client,
    table: str,
    season: int,
    rows: Sequence[Dict[str, Any]],
    entity_field: str = "id",
) -> int:
    if not rows:
        return 0

    _ensure_dataset(client, table)
    table_id = _ensure_table(client, table)

    now = datetime.now(timezone.utc).isoformat()
    payload_rows = [
        {
            "ingested_at": now,
            "season": season,
            "entity_id": str(r.get(entity_field) or ""),
            "payload": json.dumps(r, separators=(",", ":"), default=str),
        }
        for r in rows
    ]

    errors = client.insert_rows_json(table_id, payload_rows)
    if errors:
        raise RuntimeError(f"BigQuery insert errors for {table_id}: {errors[:3]}")

    logger.info("Wrote %d rows to %s", len(payload_rows), table_id)
    return len(payload_rows)


# ---------------------------------------------------------------------------
# Public ingest functions
# ---------------------------------------------------------------------------

def ingest_schedule(season: Optional[int] = None, dry_run: bool = False) -> Dict[str, Any]:
    """
    Fetch the MLS match schedule from mlssoccer.com and write to BigQuery.

    Parameters
    ----------
    season:  MLS season year (default: current calendar year)
    dry_run: If True, fetch and return counts but do NOT write to BigQuery.

    Returns
    -------
    Dict with keys: season, fetched, written, dry_run
    """
    if season is None:
        season = datetime.now(timezone.utc).year

    logger.info("[MLS schedule] Fetching season %d from mlssoccer.com", season)
    schedule = fetch_schedule(season)
    logger.info("[MLS schedule] Fetched %d matches", len(schedule))

    written = 0
    if not dry_run:
        client = _get_bq_client()
        written = _write_rows(client, TABLE_SCHEDULE, season, schedule, entity_field="id")

    return {
        "source": "mlssoccer.com",
        "data": "schedule",
        "season": season,
        "fetched": len(schedule),
        "written": written,
        "dry_run": dry_run,
    }


def ingest_team_stats(season: Optional[int] = None, dry_run: bool = False) -> Dict[str, Any]:
    """
    Fetch per-club season stats from mlssoccer.com and write to BigQuery.

    Parameters
    ----------
    season:  MLS season year (default: current calendar year)
    dry_run: If True, fetch and return counts but do NOT write to BigQuery.
    """
    if season is None:
        season = datetime.now(timezone.utc).year

    logger.info("[MLS team_stats] Fetching season %d from mlssoccer.com", season)
    team_stats = fetch_team_stats(season)
    logger.info("[MLS team_stats] Fetched %d clubs", len(team_stats))

    written = 0
    if not dry_run:
        client = _get_bq_client()
        written = _write_rows(client, TABLE_TEAM_STATS, season, team_stats, entity_field="id")

    return {
        "source": "mlssoccer.com",
        "data": "team_stats",
        "season": season,
        "fetched": len(team_stats),
        "written": written,
        "dry_run": dry_run,
    }


def ingest_player_stats(season: Optional[int] = None, dry_run: bool = False) -> Dict[str, Any]:
    """
    Fetch per-player season stats from mlssoccer.com and write to BigQuery.

    Parameters
    ----------
    season:  MLS season year (default: current calendar year)
    dry_run: If True, fetch and return counts but do NOT write to BigQuery.
    """
    if season is None:
        season = datetime.now(timezone.utc).year

    logger.info("[MLS player_stats] Fetching season %d from mlssoccer.com", season)
    player_stats = fetch_player_stats(season)
    logger.info("[MLS player_stats] Fetched %d players", len(player_stats))

    written = 0
    if not dry_run:
        client = _get_bq_client()
        written = _write_rows(client, TABLE_PLAYER_STATS, season, player_stats, entity_field="id")

    return {
        "source": "mlssoccer.com",
        "data": "player_stats",
        "season": season,
        "fetched": len(player_stats),
        "written": written,
        "dry_run": dry_run,
    }


def run_website_ingestion(
    season: Optional[int] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Run all three mlssoccer.com ingests (schedule, team stats, player stats)
    for the given season in a single call.
    """
    if season is None:
        season = datetime.now(timezone.utc).year

    schedule_result = ingest_schedule(season=season, dry_run=dry_run)
    team_stats_result = ingest_team_stats(season=season, dry_run=dry_run)
    player_stats_result = ingest_player_stats(season=season, dry_run=dry_run)

    return {
        "season": season,
        "dry_run": dry_run,
        "schedule": schedule_result,
        "team_stats": team_stats_result,
        "player_stats": player_stats_result,
    }

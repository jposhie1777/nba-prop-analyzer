"""
BigQuery ingest layer for premierleague.com scraped data.

Writes five datasets to BigQuery:
  - epl_data.premierleague_schedule
  - epl_data.premierleague_team_stats
  - epl_data.premierleague_player_stats
  - epl_data.premierleague_team_game_stats
  - epl_data.premierleague_player_game_stats
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Optional, Sequence

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

try:
    from .premierleague_scraper import (
        fetch_player_game_stats,
        fetch_player_stats,
        fetch_schedule,
        fetch_team_game_stats,
        fetch_team_stats,
    )
except ImportError:
    from mobile_api.ingest.epl.premierleague_scraper import (
        fetch_player_game_stats,
        fetch_player_stats,
        fetch_schedule,
        fetch_team_game_stats,
        fetch_team_stats,
    )

logger = logging.getLogger(__name__)

DATASET = os.getenv("EPL_WEBSITE_DATASET", "epl_data")
LOCATION = os.getenv("EPL_BQ_LOCATION", "US")

TABLE_SCHEDULE = os.getenv("EPL_WEBSITE_SCHEDULE_TABLE", f"{DATASET}.premierleague_schedule")
TABLE_TEAM_STATS = os.getenv("EPL_WEBSITE_TEAM_STATS_TABLE", f"{DATASET}.premierleague_team_stats")
TABLE_PLAYER_STATS = os.getenv("EPL_WEBSITE_PLAYER_STATS_TABLE", f"{DATASET}.premierleague_player_stats")
TABLE_TEAM_GAME_STATS = os.getenv("EPL_WEBSITE_TEAM_GAME_STATS_TABLE", f"{DATASET}.premierleague_team_game_stats")
TABLE_PLAYER_GAME_STATS = os.getenv("EPL_WEBSITE_PLAYER_GAME_STATS_TABLE", f"{DATASET}.premierleague_player_game_stats")


def _get_bq_client() -> bigquery.Client:
    project = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
    return bigquery.Client(project=project) if project else bigquery.Client()


def _table_id(client: bigquery.Client, table: str) -> str:
    if table.count(".") == 2:
        return table
    return f"{client.project}.{table}"


def _ensure_dataset(client: bigquery.Client, table: str) -> None:
    table_id = _table_id(client, table)
    dataset_id = ".".join(table_id.split(".")[:2])
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

    ingested_at = datetime.now(timezone.utc).isoformat()
    payload_rows = [
        {
            "ingested_at": ingested_at,
            "season": season,
            "entity_id": str(row.get(entity_field) or ""),
            "payload": json.dumps(row, separators=(",", ":"), default=str),
        }
        for row in rows
    ]

    chunk_size = 500
    for i in range(0, len(payload_rows), chunk_size):
        chunk = payload_rows[i : i + chunk_size]
        errors = client.insert_rows_json(table_id, chunk)
        if errors:
            raise RuntimeError(f"BigQuery insert errors for {table_id}: {errors[:3]}")

    logger.info("Wrote %d rows to %s", len(payload_rows), table_id)
    return len(payload_rows)


def ingest_schedule(season: Optional[int] = None, dry_run: bool = False) -> Dict[str, Any]:
    if season is None:
        season = datetime.now(timezone.utc).year

    rows = fetch_schedule(season)
    written = 0
    if not dry_run:
        written = _write_rows(_get_bq_client(), TABLE_SCHEDULE, season, rows)

    return {
        "source": "premierleague.com",
        "data": "schedule",
        "season": season,
        "fetched": len(rows),
        "written": written,
        "dry_run": dry_run,
    }


def ingest_team_stats(season: Optional[int] = None, dry_run: bool = False) -> Dict[str, Any]:
    if season is None:
        season = datetime.now(timezone.utc).year

    rows = fetch_team_stats(season)
    written = 0
    if not dry_run:
        written = _write_rows(_get_bq_client(), TABLE_TEAM_STATS, season, rows)

    return {
        "source": "premierleague.com",
        "data": "team_stats",
        "season": season,
        "fetched": len(rows),
        "written": written,
        "dry_run": dry_run,
    }


def ingest_player_stats(season: Optional[int] = None, dry_run: bool = False) -> Dict[str, Any]:
    if season is None:
        season = datetime.now(timezone.utc).year

    rows = fetch_player_stats(season)
    written = 0
    if not dry_run:
        written = _write_rows(_get_bq_client(), TABLE_PLAYER_STATS, season, rows)

    return {
        "source": "premierleague.com",
        "data": "player_stats",
        "season": season,
        "fetched": len(rows),
        "written": written,
        "dry_run": dry_run,
    }


def ingest_team_game_stats(
    season: Optional[int] = None,
    only_date: Optional[date] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    if season is None:
        season = datetime.now(timezone.utc).year

    rows = fetch_team_game_stats(season, only_date=only_date)
    for row in rows:
        match_id = row.get("match_id") or row.get("matchId") or ""
        team_id = row.get("team_id") or row.get("club_id") or row.get("teamId") or row.get("id") or ""
        row["_entity_id"] = f"{match_id}_{team_id}"

    written = 0
    if not dry_run:
        written = _write_rows(_get_bq_client(), TABLE_TEAM_GAME_STATS, season, rows, entity_field="_entity_id")

    return {
        "source": "premierleague.com",
        "data": "team_game_stats",
        "season": season,
        "fetched": len(rows),
        "written": written,
        "dry_run": dry_run,
    }


def ingest_player_game_stats(
    season: Optional[int] = None,
    only_date: Optional[date] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    if season is None:
        season = datetime.now(timezone.utc).year

    rows = fetch_player_game_stats(season, only_date=only_date)
    for row in rows:
        match_id = row.get("match_id") or row.get("matchId") or ""
        player_id = row.get("player_id") or row.get("playerId") or row.get("id") or ""
        row["_entity_id"] = f"{match_id}_{player_id}"

    written = 0
    if not dry_run:
        written = _write_rows(_get_bq_client(), TABLE_PLAYER_GAME_STATS, season, rows, entity_field="_entity_id")

    return {
        "source": "premierleague.com",
        "data": "player_game_stats",
        "season": season,
        "fetched": len(rows),
        "written": written,
        "dry_run": dry_run,
    }


def run_website_ingestion(season: Optional[int] = None, dry_run: bool = False) -> Dict[str, Any]:
    if season is None:
        season = datetime.now(timezone.utc).year

    return {
        "season": season,
        "dry_run": dry_run,
        "schedule": ingest_schedule(season=season, dry_run=dry_run),
        "team_stats": ingest_team_stats(season=season, dry_run=dry_run),
        "player_stats": ingest_player_stats(season=season, dry_run=dry_run),
        "team_game_stats": ingest_team_game_stats(season=season, dry_run=dry_run),
        "player_game_stats": ingest_player_game_stats(season=season, dry_run=dry_run),
    }


def run_website_backfill(
    start_season: Optional[int] = None,
    end_season: Optional[int] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    current_year = datetime.now(timezone.utc).year
    if start_season is None:
        start_season = current_year - 2
    if end_season is None:
        end_season = current_year

    seasons = list(range(start_season, end_season + 1))
    by_season: Dict[int, Any] = {}

    for season in seasons:
        by_season[season] = run_website_ingestion(season=season, dry_run=dry_run)

    return {
        "start_season": start_season,
        "end_season": end_season,
        "seasons_requested": seasons,
        "dry_run": dry_run,
        "by_season": by_season,
    }


def run_daily_ingestion(target_date: Optional[date] = None, dry_run: bool = False) -> Dict[str, Any]:
    if target_date is None:
        target_date = (datetime.now(timezone.utc) - timedelta(days=1)).date()

    season = target_date.year
    return {
        "target_date": target_date.isoformat(),
        "season": season,
        "dry_run": dry_run,
        "schedule": ingest_schedule(season=season, dry_run=dry_run),
        "team_stats": ingest_team_stats(season=season, dry_run=dry_run),
        "player_stats": ingest_player_stats(season=season, dry_run=dry_run),
        "team_game_stats": ingest_team_game_stats(season=season, only_date=target_date, dry_run=dry_run),
        "player_game_stats": ingest_player_game_stats(season=season, only_date=target_date, dry_run=dry_run),
    }


def _cli_main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Premier League website ingest")
    parser.add_argument("--mode", choices=["backfill", "season", "daily"], default="backfill")
    parser.add_argument("--start-season", type=int, default=None)
    parser.add_argument("--end-season", type=int, default=None)
    parser.add_argument("--season", type=int, default=None)
    parser.add_argument("--date", type=str, default=None)
    parser.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()

    if args.mode == "backfill":
        result = run_website_backfill(
            start_season=args.start_season,
            end_season=args.end_season,
            dry_run=args.dry_run,
        )
    elif args.mode == "daily":
        target_date = date.fromisoformat(args.date) if args.date else None
        result = run_daily_ingestion(target_date=target_date, dry_run=args.dry_run)
    else:
        result = run_website_ingestion(season=args.season, dry_run=args.dry_run)

    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    _cli_main()

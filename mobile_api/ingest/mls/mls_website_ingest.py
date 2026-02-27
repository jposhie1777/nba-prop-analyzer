"""
BigQuery ingest layer for mlssoccer.com scraped data.

Writes five datasets to BigQuery:
  - mls_data.mlssoccer_schedule          (match schedule from stats-api.mlssoccer.com)
  - mls_data.mlssoccer_team_stats        (per-club season aggregate stats)
  - mls_data.mlssoccer_player_stats      (per-player season aggregate stats)
  - mls_data.mlssoccer_team_game_stats   (per-club per-match stats)
  - mls_data.mlssoccer_player_game_stats (per-player per-match stats)

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
    from .mlssoccer_scraper import (
        fetch_schedule,
        fetch_team_stats,
        fetch_player_stats,
        fetch_team_game_stats,
        fetch_player_game_stats,
    )
except ImportError:
    from mobile_api.ingest.mls.mlssoccer_scraper import (
        fetch_schedule,
        fetch_team_stats,
        fetch_player_stats,
        fetch_team_game_stats,
        fetch_player_game_stats,
    )

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DATASET = os.getenv("MLS_DATASET", "mls_data")
LOCATION = os.getenv("MLS_BQ_LOCATION", "US")

TABLE_SCHEDULE = os.getenv("MLS_SCHEDULE_TABLE", f"{DATASET}.mlssoccer_schedule")
TABLE_TEAM_STATS = os.getenv("MLS_TEAM_STATS_TABLE", f"{DATASET}.mlssoccer_team_stats")
TABLE_PLAYER_STATS = os.getenv("MLS_PLAYER_STATS_TABLE", f"{DATASET}.mlssoccer_player_stats")
TABLE_TEAM_GAME_STATS = os.getenv("MLS_TEAM_GAME_STATS_TABLE", f"{DATASET}.mlssoccer_team_game_stats")
TABLE_PLAYER_GAME_STATS = os.getenv("MLS_PLAYER_GAME_STATS_TABLE", f"{DATASET}.mlssoccer_player_game_stats")


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

    chunk_size = 500
    for i in range(0, len(payload_rows), chunk_size):
        chunk = payload_rows[i : i + chunk_size]
        errors = client.insert_rows_json(table_id, chunk)
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


def ingest_team_game_stats(season: Optional[int] = None, dry_run: bool = False) -> Dict[str, Any]:
    """
    Fetch per-club per-match stats from mlssoccer.com and write to BigQuery.

    One row per club per completed match: possession, shots, passes, corners,
    fouls, goals, etc.  entity_id is set to "<match_id>_<club_id>" so each
    club-match combination is a unique key.

    Parameters
    ----------
    season:  MLS season year (default: current calendar year)
    dry_run: If True, fetch and return counts but do NOT write to BigQuery.
    """
    if season is None:
        season = datetime.now(timezone.utc).year

    logger.info("[MLS team_game_stats] Fetching season %d from mlssoccer.com", season)
    rows = fetch_team_game_stats(season)
    logger.info("[MLS team_game_stats] Fetched %d rows", len(rows))

    # Build a compound key so each club-per-match row is uniquely addressable.
    for row in rows:
        match_id = row.get("match_id") or row.get("matchId") or ""
        club_id = row.get("club_id") or row.get("clubId") or row.get("id") or ""
        row["_entity_id"] = f"{match_id}_{club_id}"

    written = 0
    if not dry_run:
        client = _get_bq_client()
        written = _write_rows(client, TABLE_TEAM_GAME_STATS, season, rows, entity_field="_entity_id")

    return {
        "source": "mlssoccer.com",
        "data": "team_game_stats",
        "season": season,
        "fetched": len(rows),
        "written": written,
        "dry_run": dry_run,
    }


def ingest_player_game_stats(season: Optional[int] = None, dry_run: bool = False) -> Dict[str, Any]:
    """
    Fetch per-player per-match stats from mlssoccer.com and write to BigQuery.

    One row per player per completed match: minutes, goals, assists, shots,
    passes, key passes, tackles, yellow/red cards, etc.  entity_id is set to
    "<match_id>_<player_id>" so each player-match combination is a unique key.

    Parameters
    ----------
    season:  MLS season year (default: current calendar year)
    dry_run: If True, fetch and return counts but do NOT write to BigQuery.
    """
    if season is None:
        season = datetime.now(timezone.utc).year

    logger.info("[MLS player_game_stats] Fetching season %d from mlssoccer.com", season)
    rows = fetch_player_game_stats(season)
    logger.info("[MLS player_game_stats] Fetched %d rows", len(rows))

    # Build a compound key so each player-per-match row is uniquely addressable.
    for row in rows:
        match_id = row.get("match_id") or row.get("matchId") or ""
        player_id = row.get("player_id") or row.get("playerId") or row.get("id") or ""
        row["_entity_id"] = f"{match_id}_{player_id}"

    written = 0
    if not dry_run:
        client = _get_bq_client()
        written = _write_rows(client, TABLE_PLAYER_GAME_STATS, season, rows, entity_field="_entity_id")

    return {
        "source": "mlssoccer.com",
        "data": "player_game_stats",
        "season": season,
        "fetched": len(rows),
        "written": written,
        "dry_run": dry_run,
    }


def run_website_ingestion(
    season: Optional[int] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Run all five mlssoccer.com ingests in a single call:
    schedule, team stats, player stats, team game stats, player game stats.
    """
    if season is None:
        season = datetime.now(timezone.utc).year

    schedule_result = ingest_schedule(season=season, dry_run=dry_run)
    team_stats_result = ingest_team_stats(season=season, dry_run=dry_run)
    player_stats_result = ingest_player_stats(season=season, dry_run=dry_run)
    team_game_stats_result = ingest_team_game_stats(season=season, dry_run=dry_run)
    player_game_stats_result = ingest_player_game_stats(season=season, dry_run=dry_run)

    return {
        "season": season,
        "dry_run": dry_run,
        "schedule": schedule_result,
        "team_stats": team_stats_result,
        "player_stats": player_stats_result,
        "team_game_stats": team_game_stats_result,
        "player_game_stats": player_game_stats_result,
    }


def run_website_backfill(
    start_season: Optional[int] = None,
    end_season: Optional[int] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Backfill all five mlssoccer.com feeds for a range of seasons.

    Iterates start_season..end_season inclusive, running all five ingest
    functions for each year.  Defaults to the two seasons before the current
    year through the current year (e.g. 2024–2026 when called in 2026).

    Parameters
    ----------
    start_season: First season to backfill (default: current_year - 2)
    end_season:   Last season to backfill  (default: current_year)
    dry_run:      If True, fetch data but do NOT write to BigQuery.

    Returns
    -------
    Dict keyed by season year, each containing the five per-feed results.
    """
    current_year = datetime.now(timezone.utc).year
    if start_season is None:
        start_season = current_year - 2
    if end_season is None:
        end_season = current_year

    seasons = list(range(start_season, end_season + 1))
    logger.info(
        "[MLS backfill] Starting backfill for seasons %s (dry_run=%s)",
        seasons,
        dry_run,
    )

    results: Dict[str, Any] = {
        "start_season": start_season,
        "end_season": end_season,
        "seasons_requested": seasons,
        "dry_run": dry_run,
        "by_season": {},
    }

    for season in seasons:
        logger.info("[MLS backfill] Processing season %d", season)
        season_result = run_website_ingestion(season=season, dry_run=dry_run)
        results["by_season"][season] = season_result
        logger.info(
            "[MLS backfill] Season %d complete — schedule: %s, team_stats: %s, "
            "player_stats: %s, team_game_stats: %s, player_game_stats: %s",
            season,
            season_result["schedule"]["fetched"],
            season_result["team_stats"]["fetched"],
            season_result["player_stats"]["fetched"],
            season_result["team_game_stats"]["fetched"],
            season_result["player_game_stats"]["fetched"],
        )

    return results


# ---------------------------------------------------------------------------
# CLI entry-point  (used for backfill and ad-hoc runs)
#
# Examples
# --------
#   # Dry-run backfill 2024–2026, prints counts without writing
#   python -m mobile_api.ingest.mls.mls_website_ingest --mode backfill --start-season 2024 --end-season 2026 --dry-run
#
#   # Live backfill 2024–2026
#   python -m mobile_api.ingest.mls.mls_website_ingest --mode backfill --start-season 2024 --end-season 2026
#
#   # Refresh current season only
#   python -m mobile_api.ingest.mls.mls_website_ingest --mode season --season 2026
# ---------------------------------------------------------------------------

import argparse as _argparse


def _cli_main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    parser = _argparse.ArgumentParser(
        description="MLS mlssoccer.com ingest — backfill or single-season run"
    )
    parser.add_argument(
        "--mode",
        choices=["backfill", "season"],
        default="backfill",
        help="'backfill' iterates a season range; 'season' runs a single season (default: backfill)",
    )
    parser.add_argument(
        "--start-season",
        type=int,
        default=2024,
        help="First season to backfill (backfill mode, default: 2024)",
    )
    parser.add_argument(
        "--end-season",
        type=int,
        default=2026,
        help="Last season to backfill (backfill mode, default: 2026)",
    )
    parser.add_argument(
        "--season",
        type=int,
        default=None,
        help="Season year for single-season mode (default: current year)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch data but do NOT write to BigQuery",
    )

    args = parser.parse_args()

    if args.mode == "backfill":
        result = run_website_backfill(
            start_season=args.start_season,
            end_season=args.end_season,
            dry_run=args.dry_run,
        )
        print(json.dumps(result, indent=2, default=str))
    else:
        result = run_website_ingestion(
            season=args.season,
            dry_run=args.dry_run,
        )
        print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    _cli_main()

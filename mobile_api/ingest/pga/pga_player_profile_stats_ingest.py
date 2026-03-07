"""
Ingest PGA Tour per-player profile stats → BigQuery.

For every active player found in the current season's statOverview leaderboard,
fetches their individual stats page on pgatour.com and stores the ``statsOverview``
key stats (value, rank, rank-deviation, category) in the ``player_profile_stats``
BigQuery table.

This is complementary to ``pga_stats_ingest`` which queries the stat leaderboards.
The player-centric approach captures stats for every active player regardless of
whether they appear in individual stat leaderboards (which require a minimum
number of measured rounds).

Usage (standalone CLI):
    python -m mobile_api.ingest.pga.pga_player_profile_stats_ingest --season 2026 --tour R
    python -m mobile_api.ingest.pga.pga_player_profile_stats_ingest --season 2026 --dry-run
"""

from __future__ import annotations

import argparse
import datetime
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional

from google.api_core.exceptions import BadRequest
from google.cloud import bigquery

from .pga_player_profile_stats_scraper import (
    PlayerProfileStatRow,
    fetch_player_profile_stats,
    profile_stats_to_records,
)
from .pga_stats_scraper import fetch_stat_overview

DATASET = os.getenv("PGA_DATASET", "pga_data")
TABLE = os.getenv("PGA_PLAYER_PROFILE_STATS_TABLE", "website_player_profile_stats")
CHUNK_SIZE = 500

# Concurrency: fetch multiple players in parallel, but stay polite
MAX_WORKERS = int(os.getenv("PGA_PROFILE_WORKERS", "4"))
# Minimum seconds to sleep between each player fetch (per worker)
REQUEST_DELAY = float(os.getenv("PGA_PROFILE_REQUEST_DELAY", "0.5"))

_SCHEMA = [
    bigquery.SchemaField("run_ts", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("tour_code", "STRING"),
    bigquery.SchemaField("season", "INTEGER"),
    bigquery.SchemaField("player_id", "STRING"),
    bigquery.SchemaField("player_name", "STRING"),
    bigquery.SchemaField("stat_id", "STRING"),
    bigquery.SchemaField("stat_title", "STRING"),
    bigquery.SchemaField("stat_value", "STRING"),
    bigquery.SchemaField("rank", "INTEGER"),
    bigquery.SchemaField("rank_deviation", "FLOAT"),
    bigquery.SchemaField("above_or_below", "STRING"),
    bigquery.SchemaField("categories", "STRING"),  # JSON array, e.g. '["STROKES_GAINED"]'
]


# ---------------------------------------------------------------------------
# BigQuery helpers
# ---------------------------------------------------------------------------


def _bq_client() -> bigquery.Client:
    project = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
    return bigquery.Client(project=project)


def _ensure_table(client: bigquery.Client) -> str:
    """Create the player_profile_stats table if it doesn't exist; return full table ID."""
    bq_table = bigquery.Table(
        f"{client.project}.{DATASET}.{TABLE}",
        schema=_SCHEMA,
    )
    bq_table.range_partitioning = bigquery.RangePartitioning(
        field="season",
        range_=bigquery.PartitionRange(start=2015, end=2035, interval=1),
    )
    bq_table.clustering_fields = ["tour_code", "player_id", "stat_id"]
    bq_table.description = (
        "PGA Tour per-player profile stats scraped from pgatour.com "
        "player stats pages (__NEXT_DATA__ statsOverview)."
    )
    client.create_table(bq_table, exists_ok=True)
    return f"{client.project}.{DATASET}.{TABLE}"


# ---------------------------------------------------------------------------
# Player list helper
# ---------------------------------------------------------------------------


def _get_active_players(
    tour_code: str,
    season: int,
) -> List[Dict[str, str]]:
    """
    Return a deduplicated list of {player_id, player_name} dicts for all players
    who appear in at least one stat leaderboard this season.
    """
    print(
        f"[profile_stats] Fetching active player list from statOverview "
        f"({tour_code}/{season})…",
        flush=True,
    )
    result = fetch_stat_overview(tour_code=tour_code, year=season)
    seen: set[str] = set()
    players: List[Dict[str, str]] = []
    for p in result.players:
        if p.player_id and p.player_id not in seen:
            seen.add(p.player_id)
            players.append({"player_id": p.player_id, "player_name": p.player_name})
    print(
        f"[profile_stats] Found {len(players)} unique active players.",
        flush=True,
    )
    return players


# ---------------------------------------------------------------------------
# Per-player fetch (used in thread pool)
# ---------------------------------------------------------------------------


def _fetch_one(
    player: Dict[str, str],
    season: int,
    tour_code: str,
) -> List[PlayerProfileStatRow]:
    """Fetch profile stats for one player; return empty list on soft failures."""
    pid = player["player_id"]
    name = player["player_name"]
    try:
        time.sleep(REQUEST_DELAY)
        rows = fetch_player_profile_stats(
            player_id=pid,
            player_name=name,
            season=season,
            tour_code=tour_code,
        )
        return rows
    except Exception as exc:
        print(
            f"[profile_stats] WARN: skipping player {pid} ({name}): {exc}",
            flush=True,
        )
        return []


# ---------------------------------------------------------------------------
# Main ingest
# ---------------------------------------------------------------------------


def ingest_player_profile_stats(
    season: int,
    tour_code: str = "R",
    dry_run: bool = False,
    create_tables: bool = True,
    run_ts: Optional[str] = None,
    players: Optional[List[Dict[str, str]]] = None,
) -> dict:
    """
    Fetch and store profile stats for every active player.

    Args:
        season:        Season year (e.g. 2026).
        tour_code:     Tour code, e.g. ``"R"`` (PGA Tour).
        dry_run:       If True, fetch data but skip BigQuery writes.
        create_tables: Auto-create the BigQuery table if it doesn't exist.
        run_ts:        ISO-8601 timestamp for run_ts / ingested_at fields.
        players:       Optional explicit list of ``{player_id, player_name}``
                       dicts; if omitted the list is built from statOverview.

    Returns:
        Dict with ``players_fetched``, ``rows_fetched``, ``rows_inserted`` counts.
    """
    ts = run_ts or datetime.datetime.utcnow().isoformat()

    if players is None:
        players = _get_active_players(tour_code=tour_code, season=season)

    if not players:
        print("[profile_stats] No active players found — aborting.", flush=True)
        return {"players_fetched": 0, "rows_fetched": 0, "rows_inserted": 0}

    # Fetch all players concurrently (bounded by MAX_WORKERS)
    all_rows: List[PlayerProfileStatRow] = []
    completed = 0
    total = len(players)

    print(
        f"[profile_stats] Fetching profile stats for {total} players "
        f"(workers={MAX_WORKERS}, delay={REQUEST_DELAY}s)…",
        flush=True,
    )

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(_fetch_one, p, season, tour_code): p
            for p in players
        }
        for future in as_completed(futures):
            rows = future.result()
            all_rows.extend(rows)
            completed += 1
            if completed % 25 == 0 or completed == total:
                print(
                    f"[profile_stats] {completed}/{total} players done, "
                    f"{len(all_rows)} stat rows so far.",
                    flush=True,
                )

    records = profile_stats_to_records(all_rows, run_ts=ts)
    rows_fetched = len(records)
    print(
        f"[profile_stats] Fetched {rows_fetched} stat rows for "
        f"{total} players ({tour_code}/{season}).",
        flush=True,
    )

    if dry_run or not records:
        return {
            "players_fetched": total,
            "rows_fetched": rows_fetched,
            "rows_inserted": 0,
        }

    client = _bq_client()
    if create_tables:
        table_id = _ensure_table(client)
    else:
        table_id = f"{client.project}.{DATASET}.{TABLE}"

    # Replace the snapshot for this tour_code + season
    delete_sql = (
        f"DELETE FROM `{table_id}` "
        "WHERE tour_code = @tour_code AND season = @season"
    )
    query_params = [
        bigquery.ScalarQueryParameter("tour_code", "STRING", tour_code),
        bigquery.ScalarQueryParameter("season", "INT64", int(season)),
    ]
    delete_cfg = bigquery.QueryJobConfig(query_parameters=query_params)

    try:
        client.query(delete_sql, job_config=delete_cfg).result()
    except BadRequest as exc:
        if "streaming buffer" in str(exc) and "not supported" in str(exc):
            print(
                "[profile_stats] Delete skipped (streaming buffer); "
                "new rows will append.",
                flush=True,
            )
        else:
            raise

    inserted = 0
    for i in range(0, len(records), CHUNK_SIZE):
        chunk = records[i: i + CHUNK_SIZE]
        errors = client.insert_rows_json(table_id, chunk)
        if errors:
            raise RuntimeError(f"BigQuery insert errors: {errors}")
        inserted += len(chunk)
        time.sleep(0.05)

    print(
        f"[profile_stats] Inserted {inserted} rows into {table_id}.",
        flush=True,
    )
    return {
        "players_fetched": total,
        "rows_fetched": rows_fetched,
        "rows_inserted": inserted,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _cli() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest PGA player profile stats to BigQuery."
    )
    parser.add_argument(
        "--season", type=int,
        default=datetime.datetime.utcnow().year,
        help="Season year (default: current year)",
    )
    parser.add_argument("--tour", default="R", metavar="TOUR_CODE")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-create-table", action="store_true")
    args = parser.parse_args()

    result = ingest_player_profile_stats(
        season=args.season,
        tour_code=args.tour,
        dry_run=args.dry_run,
        create_tables=not args.no_create_table,
    )
    print(result)


if __name__ == "__main__":
    _cli()

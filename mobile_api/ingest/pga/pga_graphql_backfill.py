"""
PGA Tour GraphQL backfill → BigQuery.

Replaces the BallDontLie-based backfill.py. Uses the PGA Tour GraphQL API
exclusively:

  - schedule(tourCode, year)     → tournaments table  (STRING IDs)
  - leaderboardV3(id) per event  → tournament_results + players tables
  - statOverview(tourCode, year) → player_stats table  (existing ingest)
  - priorityRankings(…)          → priority_rankings   (existing ingest)

Round scores and hole-by-hole scorecards come from pga_scorecards_ingest.py
(scorecardV3 per player per tournament), which also populates
tournament_round_scores.

Usage:
    python mobile_api/ingest/pga/pga_graphql_backfill.py
    # controlled via env vars PGA_START_SEASON / PGA_END_SEASON / PGA_BACKFILL_YEARS
"""

from __future__ import annotations

import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from google.cloud import bigquery

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from mobile_api.ingest.pga.pga_leaderboard import LeaderboardPlayer, fetch_leaderboard
from mobile_api.ingest.pga.pga_schedule import ScheduleTournament, fetch_schedule

DATASET = os.getenv("PGA_DATASET", "pga_data")
LOCATION = os.getenv("PGA_DATASET_LOCATION", "US")
PROJECT = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
TOUR_CODE = os.getenv("PGA_TOUR_CODE", "R")


# ---------------------------------------------------------------------------
# BQ helpers
# ---------------------------------------------------------------------------


def _get_client() -> bigquery.Client:
    return bigquery.Client(project=PROJECT) if PROJECT else bigquery.Client()


def _ensure_dataset(client: bigquery.Client) -> None:
    ds = bigquery.Dataset(f"{client.project}.{DATASET}")
    ds.location = LOCATION
    client.create_dataset(ds, exists_ok=True)


_TABLES: list = []  # populated by _register_table calls

def _register_table(
    name: str,
    schema: List[bigquery.SchemaField],
    cluster_fields: Optional[List[str]] = None,
) -> None:
    _TABLES.append((name, schema, cluster_fields))


_register_table(
    "players",
    [
        bigquery.SchemaField("run_ts", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("player_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("display_name", "STRING"),
        bigquery.SchemaField("first_name", "STRING"),
        bigquery.SchemaField("last_name", "STRING"),
        bigquery.SchemaField("country", "STRING"),
        bigquery.SchemaField("amateur", "BOOL"),
    ],
    ["player_id"],
)

_register_table(
    "tournaments",
    [
        bigquery.SchemaField("run_ts", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("tournament_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("tour_code", "STRING"),
        bigquery.SchemaField("season", "INT64"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("start_date", "STRING"),
        bigquery.SchemaField("city", "STRING"),
        bigquery.SchemaField("state", "STRING"),
        bigquery.SchemaField("country", "STRING"),
        bigquery.SchemaField("purse", "STRING"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("champion", "STRING"),
    ],
    ["tournament_id", "season"],
)

_register_table(
    "tournament_results",
    [
        bigquery.SchemaField("run_ts", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("season", "INT64"),
        bigquery.SchemaField("tournament_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("tournament_name", "STRING"),
        bigquery.SchemaField("tournament_start_date", "STRING"),
        bigquery.SchemaField("player_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("player_display_name", "STRING"),
        bigquery.SchemaField("position", "STRING"),
        bigquery.SchemaField("position_numeric", "INT64"),
        bigquery.SchemaField("par_relative_score", "INT64"),
        bigquery.SchemaField("total_strokes", "INT64"),
    ],
    ["tournament_id", "player_id"],
)

_register_table(
    "tournament_round_scores",
    [
        bigquery.SchemaField("run_ts", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("season", "INT64"),
        bigquery.SchemaField("tournament_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("tournament_name", "STRING"),
        bigquery.SchemaField("tournament_start_date", "STRING"),
        bigquery.SchemaField("round_number", "INT64"),
        bigquery.SchemaField("player_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("player_display_name", "STRING"),
        bigquery.SchemaField("round_score", "INT64"),
        bigquery.SchemaField("par_relative_score", "INT64"),
        bigquery.SchemaField("total_score", "INT64"),
    ],
    ["tournament_id", "player_id", "round_number"],
)


def _ensure_tables(client: bigquery.Client, *, drop_existing: bool = False) -> None:
    """Create (or recreate) the required BQ tables.

    When ``drop_existing=True`` (i.e. the backfill runs with PGA_TRUNCATE=true)
    the old tables are dropped first so that schema changes (INT64→STRING IDs,
    added columns, changed column types) take effect cleanly.
    """
    for name, schema, cluster in _TABLES:
        table_ref = f"{client.project}.{DATASET}.{name}"
        if drop_existing:
            client.delete_table(table_ref, not_found_ok=True)
        bq_table = bigquery.Table(table_ref, schema=schema)
        if cluster:
            bq_table.clustering_fields = cluster
        client.create_table(bq_table, exists_ok=True)


def _insert(
    client: bigquery.Client,
    table: str,
    rows: List[Dict[str, Any]],
    chunk: int = 500,
) -> None:
    if not rows:
        return
    table_id = f"{client.project}.{DATASET}.{table}"
    for i in range(0, len(rows), chunk):
        errors = client.insert_rows_json(table_id, rows[i : i + chunk])
        if errors:
            raise RuntimeError(f"BQ insert errors for {table}: {errors}")
        time.sleep(0.05)


# ---------------------------------------------------------------------------
# Normalisation helpers
# ---------------------------------------------------------------------------


def _parse_score(s: Optional[str]) -> Optional[int]:
    """Parse PGA Tour score-to-par string: 'E' → 0, '-4' → -4, '+2' → 2."""
    if s is None:
        return None
    s = s.strip()
    if s in ("E", "EVEN", ""):
        return 0
    try:
        return int(s.lstrip("+"))
    except ValueError:
        return None


def _parse_position_numeric(pos: Optional[str]) -> Optional[int]:
    """Parse position string 'T2', '1', 'CUT', 'WD' → int or None."""
    if not pos:
        return None
    cleaned = pos.strip().lstrip("T")
    try:
        return int(cleaned)
    except ValueError:
        return None


def normalize_tournaments(
    tournaments: List[ScheduleTournament],
    season: int,
    tour_code: str,
    run_ts: str,
) -> List[Dict[str, Any]]:
    rows = []
    for t in tournaments:
        rows.append({
            "run_ts": run_ts,
            "ingested_at": run_ts,
            "tournament_id": t.tournament_id,
            "tour_code": tour_code,
            "season": season,
            "name": t.name,
            "start_date": t.start_date,
            "city": t.city,
            "state": t.state,
            "country": t.country,
            "purse": t.purse,
            "status": t.status_type,
            "champion": t.champion,
        })
    return rows


def normalize_players(
    players: Iterable[LeaderboardPlayer],
    run_ts: str,
) -> List[Dict[str, Any]]:
    seen: set = set()
    rows = []
    for p in players:
        if not p.player_id or p.player_id in seen:
            continue
        seen.add(p.player_id)
        rows.append({
            "run_ts": run_ts,
            "ingested_at": run_ts,
            "player_id": p.player_id,
            "display_name": p.display_name,
            "first_name": p.first_name,
            "last_name": p.last_name,
            "country": p.country,
            "amateur": p.amateur,
        })
    return rows


def normalize_results(
    pairs: List[tuple],   # (ScheduleTournament, LeaderboardPlayer)
    run_ts: str,
) -> List[Dict[str, Any]]:
    rows = []
    for t, p in pairs:
        rows.append({
            "run_ts": run_ts,
            "ingested_at": run_ts,
            "season": None,          # filled below
            "tournament_id": t.tournament_id,
            "tournament_name": t.name,
            "tournament_start_date": t.start_date,
            "player_id": p.player_id,
            "player_display_name": p.display_name,
            "position": p.position,
            "position_numeric": _parse_position_numeric(p.position),
            "par_relative_score": _parse_score(p.total),
            "total_strokes": int(p.total_strokes) if p.total_strokes and p.total_strokes.isdigit() else None,
        })
    return rows


def normalize_round_scores(
    pairs: List[tuple],   # (ScheduleTournament, LeaderboardPlayer, season)
    run_ts: str,
) -> List[Dict[str, Any]]:
    """
    Extract per-round par-relative scores from leaderboard `rounds` data.

    Note: this gives par-relative score per round only; absolute strokes per
    round require scorecardV3 (populated by pga_scorecards_ingest.py).
    """
    rows = []
    for t, p, season in pairs:
        for rnd in p.rounds:
            rows.append({
                "run_ts": run_ts,
                "ingested_at": run_ts,
                "season": season,
                "tournament_id": t.tournament_id,
                "tournament_name": t.name,
                "tournament_start_date": t.start_date,
                "round_number": rnd.round_number,
                "player_id": p.player_id,
                "player_display_name": p.display_name,
                "round_score": None,          # absolute strokes — from scorecardV3
                "par_relative_score": _parse_score(rnd.score),
                "total_score": None,
            })
    return rows


# ---------------------------------------------------------------------------
# Season range
# ---------------------------------------------------------------------------


def _season_range() -> List[int]:
    start = os.getenv("PGA_START_SEASON")
    end = os.getenv("PGA_END_SEASON")
    if start and end:
        return list(range(int(start), int(end) + 1))
    years_back = int(os.getenv("PGA_BACKFILL_YEARS", "5"))
    current = datetime.utcnow().year
    return [current - i for i in range(years_back - 1, -1, -1)]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    client = _get_client()
    _ensure_dataset(client)

    truncate = os.getenv("PGA_TRUNCATE", "true").lower() == "true"
    # When truncating, drop and recreate so schema changes (e.g. INT64→STRING IDs,
    # new columns) take effect. When not truncating, only create if missing.
    _ensure_tables(client, drop_existing=truncate)

    run_ts = datetime.utcnow().isoformat()
    tour_code = TOUR_CODE

    for season in _season_range():
        print(f"\n=== Season {season} ===")

        # 1. Schedule → tournaments
        print(f"  Fetching schedule for {tour_code}/{season}…")
        try:
            schedule = fetch_schedule(tour_code=tour_code, year=str(season))
        except Exception as exc:
            print(f"  [warn] schedule failed for {season}: {exc}")
            continue

        completed = [t for t in schedule if t.bucket == "completed"]
        upcoming = [t for t in schedule if t.bucket == "upcoming"]
        all_tournaments = completed + upcoming
        print(f"  {len(completed)} completed + {len(upcoming)} upcoming tournaments")

        tournament_rows = normalize_tournaments(all_tournaments, season, tour_code, run_ts)
        _insert(client, "tournaments", tournament_rows)

        # 2. Leaderboard per tournament → results + players
        all_players: Dict[str, LeaderboardPlayer] = {}
        result_pairs: List[tuple] = []
        round_pairs: List[tuple] = []

        for t in completed:
            print(f"  Leaderboard: {t.tournament_id} {t.name}")
            try:
                players = fetch_leaderboard(t.tournament_id)
            except Exception as exc:
                print(f"  [warn] leaderboard failed for {t.tournament_id}: {exc}")
                time.sleep(1.0)
                continue

            for p in players:
                all_players[p.player_id] = p
                result_pairs.append((t, p))
                round_pairs.append((t, p, season))

            time.sleep(0.5)

        # Flush players accumulated for this season
        player_rows = normalize_players(all_players.values(), run_ts)
        _insert(client, "players", player_rows)
        print(f"  Stored {len(player_rows)} players")

        # Flush results
        result_rows = normalize_results(result_pairs, run_ts)
        for row in result_rows:
            row["season"] = season
        _insert(client, "tournament_results", result_rows)
        print(f"  Stored {len(result_rows)} tournament result rows")

        # Flush per-round scores (par-relative only from leaderboard)
        round_rows = normalize_round_scores(round_pairs, run_ts)
        _insert(client, "tournament_round_scores", round_rows)
        print(f"  Stored {len(round_rows)} round score rows (par-relative; use scorecards backfill for full strokes)")

        time.sleep(1.0)

    print("\nPGA GraphQL backfill complete.")


if __name__ == "__main__":
    main()

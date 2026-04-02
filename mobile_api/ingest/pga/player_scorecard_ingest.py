"""
Ingest PGA Tour player tournament scorecard history → BigQuery.

Populates the ``website_player_scorecard`` table with one row per player
per tournament containing actual stroke counts for each round played
(R1, R2, R3, R4), the cumulative total, finishing position, and
score-to-par.  Data is scraped from each player's results page on
pgatour.com and parsed from the embedded Next.js __NEXT_DATA__ blob.

Two run modes are supported:

``run_backfill(seasons)``
    Iterates the specified season years, fetches every active player's
    full tournament history for each season, and inserts all rows into
    BigQuery.  Intended for initial population or historical re-loads.

``run_daily(tour_code, season)``
    Identifies the most recently active tournament, collects its player
    list from the leaderboard, fetches only that season's results for
    each player, and upserts the current tournament's round scores.
    Intended to run nightly after each tournament round completes.

Both modes append to the table with a ``run_ts`` timestamp.  Downstream
queries deduplicate with::

    ROW_NUMBER() OVER (PARTITION BY tournament_id, player_id ORDER BY run_ts DESC)

Usage (standalone CLI):

    # Backfill last 5 seasons
    python -m mobile_api.ingest.pga.player_scorecard_ingest --backfill

    # Backfill specific seasons
    python -m mobile_api.ingest.pga.player_scorecard_ingest --backfill --start-season 2022 --end-season 2026

    # Daily update (current season, current tournament)
    python -m mobile_api.ingest.pga.player_scorecard_ingest --daily

    # Dry-run (fetch but do not write to BQ)
    python -m mobile_api.ingest.pga.player_scorecard_ingest --daily --dry-run
"""

from __future__ import annotations

import argparse
import datetime
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from google.cloud import bigquery

from .player_scorecard_scraper import (
    PlayerTournamentScorecard,
    fetch_player_scorecard_history,
    scorecard_history_to_records,
)
from .pga_leaderboard import fetch_leaderboard
from .pga_schedule import fetch_schedule
from .pga_stats_scraper import fetch_stat_overview

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DATASET = os.getenv("PGA_DATASET", "pga_data")
DATASET_LOCATION = os.getenv("PGA_DATASET_LOCATION", "US")
TABLE = os.getenv("PGA_PLAYER_SCORECARD_TABLE", "website_player_scorecard")

CHUNK_SIZE = 500
MAX_WORKERS = int(os.getenv("PGA_SCORECARD_WORKERS", "4"))
REQUEST_DELAY = float(os.getenv("PGA_SCORECARD_REQUEST_DELAY", "0.75"))

# ---------------------------------------------------------------------------
# BigQuery schema
# ---------------------------------------------------------------------------

_SCHEMA = [
    bigquery.SchemaField("run_ts", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("season", "INTEGER"),
    bigquery.SchemaField("tournament_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("tournament_name", "STRING"),
    bigquery.SchemaField("tournament_date", "STRING"),
    bigquery.SchemaField("course_name", "STRING"),
    bigquery.SchemaField("player_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("player_display_name", "STRING"),
    bigquery.SchemaField("position", "STRING"),
    bigquery.SchemaField("r1", "INTEGER"),
    bigquery.SchemaField("r2", "INTEGER"),
    bigquery.SchemaField("r3", "INTEGER"),
    bigquery.SchemaField("r4", "INTEGER"),
    bigquery.SchemaField("total_strokes", "INTEGER"),
    bigquery.SchemaField("to_par", "STRING"),
]

# ---------------------------------------------------------------------------
# BigQuery helpers
# ---------------------------------------------------------------------------


def _bq_client() -> bigquery.Client:
    project = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
    return bigquery.Client(project=project)


def _ensure_table(client: bigquery.Client, *, truncate: bool = False) -> str:
    """
    Create the website_player_scorecard table if it does not exist.

    Args:
        truncate: If True, truncate the table after ensuring it exists.
                  Pass ``True`` at the start of a full backfill so stale
                  history is replaced rather than appended.

    Returns:
        Fully-qualified BigQuery table ID.
    """
    table_id = f"{client.project}.{DATASET}.{TABLE}"
    bq_table = bigquery.Table(table_id, schema=_SCHEMA)
    bq_table.range_partitioning = bigquery.RangePartitioning(
        field="season",
        range_=bigquery.PartitionRange(start=2000, end=2040, interval=1),
    )
    bq_table.clustering_fields = ["tournament_id", "player_id"]
    bq_table.description = (
        "PGA Tour player tournament scorecard history — 1 row per player per "
        "tournament with actual stroke counts for R1, R2, R3, R4, and the "
        "cumulative total.  Source: pgatour.com player results pages."
    )
    client.create_table(bq_table, exists_ok=True)
    _ensure_required_columns(client, table_id)
    if truncate:
        client.query(f"TRUNCATE TABLE `{table_id}`").result()
    return table_id


def _ensure_required_columns(client: bigquery.Client, table_id: str) -> None:
    """Add any columns from _SCHEMA that are missing from an existing table."""
    table = client.get_table(table_id)
    existing = {field.name.lower() for field in table.schema}
    for field in _SCHEMA:
        if field.name.lower() in existing:
            continue
        client.query(
            f"ALTER TABLE `{table_id}` ADD COLUMN IF NOT EXISTS {field.name} {field.field_type}"
        ).result()



def _insert_rows(
    client: bigquery.Client,
    table_id: str,
    rows: List[Dict[str, Any]],
) -> int:
    """Stream rows into BigQuery in chunks; return the count inserted."""
    if not rows:
        return 0
    written = 0
    for i in range(0, len(rows), CHUNK_SIZE):
        chunk = rows[i : i + CHUNK_SIZE]
        errors = client.insert_rows_json(table_id, chunk)
        if errors:
            raise RuntimeError(
                f"BigQuery insert errors for {table_id}: {errors[:3]}"
            )
        written += len(chunk)
        time.sleep(0.05)
    return written


# ---------------------------------------------------------------------------
# Player list helpers
# ---------------------------------------------------------------------------


def _get_players_from_website_active_players(
    client: Optional["bigquery.Client"] = None,
) -> List[Dict[str, str]]:
    """
    Return ``{player_id, player_name}`` dicts for all active players stored in
    the ``website_active_players`` BigQuery table.  This table is populated by
    the ``playerDirectory`` GraphQL query and contains the full tour roster
    (~500+ players), unlike the ``statOverview`` endpoint which only returns
    players with measured stat entries (~17).
    """
    import json as _json

    c = client or _bq_client()
    project = c.project
    table_id = f"{project}.{DATASET}.website_active_players"
    query = f"""
        SELECT player_id, display_name, player_payload
        FROM `{table_id}`
        WHERE active = TRUE
    """
    try:
        rows = list(c.query(query).result())
    except Exception as exc:
        print(
            f"[player_scorecard] WARN: could not query website_active_players: {exc}",
            flush=True,
        )
        return []

    seen: Set[str] = set()
    players: List[Dict[str, str]] = []
    for row in rows:
        pid = str(row.get("player_id") or "").strip()
        if not pid or pid in seen:
            continue
        seen.add(pid)
        # Prefer display_name column; fall back to payload displayName field.
        name = str(row.get("display_name") or "").strip()
        if not name:
            try:
                payload = _json.loads(row.get("player_payload") or "{}")
                name = (
                    payload.get("displayName")
                    or payload.get("playerName")
                    or payload.get("name")
                    or ""
                )
            except Exception:
                pass
        if name:
            players.append({"player_id": pid, "player_name": name})
    return players


def _get_active_players_from_stat_overview(
    tour_code: str = "R",
    season: int = 2026,
) -> List[Dict[str, str]]:
    """
    Return ``{player_id, player_name}`` dicts for all players who appear in at
    least one stat leaderboard for this season.  Used as a fallback when the
    ``website_active_players`` table is unavailable.
    """
    result = fetch_stat_overview(tour_code=tour_code, year=season)
    seen: Set[str] = set()
    players: List[Dict[str, str]] = []
    for p in result.players:
        if p.player_id and p.player_id not in seen:
            seen.add(p.player_id)
            players.append({"player_id": p.player_id, "player_name": p.player_name})
    return players


def _get_recent_tournament_players(
    tournament_id: str,
) -> List[Dict[str, str]]:
    """
    Return ``{player_id, player_name}`` dicts for all players in the given
    tournament's leaderboard.  Used during the daily run.
    """
    players_lb = fetch_leaderboard(tournament_id)
    seen: Set[str] = set()
    players: List[Dict[str, str]] = []
    for p in players_lb:
        if p.player_id and p.player_id not in seen:
            seen.add(p.player_id)
            players.append(
                {
                    "player_id": p.player_id,
                    "player_name": p.display_name or "",
                }
            )
    return players


def _get_most_recent_tournament_id(
    tour_code: str = "R",
    year: Optional[int] = None,
) -> Optional[str]:
    """
    Fetch the schedule and return the most recently started tournament ID.

    Preference order:
    1. Upcoming tournament whose start date has passed (in-progress)
    2. Most recently completed tournament
    """
    current_year = year or datetime.datetime.utcnow().year
    tournaments = fetch_schedule(tour_code=tour_code, year=str(current_year))
    today = datetime.datetime.utcnow().date()

    # Collect (start_date, tournament_id) tuples from completed and upcoming
    completed: List[tuple] = []
    upcoming_started: List[tuple] = []

    for t in tournaments:
        tid = getattr(t, "id", None) or getattr(t, "tournament_id", None) or ""
        start = getattr(t, "start_date", None) or ""
        bucket = (getattr(t, "bucket", None) or "").lower()
        if not tid or not start:
            continue
        try:
            start_date = datetime.date.fromisoformat(str(start)[:10])
        except ValueError:
            continue

        if bucket == "completed":
            completed.append((start_date, tid))
        elif bucket == "upcoming" and start_date <= today:
            upcoming_started.append((start_date, tid))

    if upcoming_started:
        return max(upcoming_started, key=lambda x: x[0])[1]
    if completed:
        return max(completed, key=lambda x: x[0])[1]
    return None


# ---------------------------------------------------------------------------
# Per-player fetch (used in thread pool)
# ---------------------------------------------------------------------------


def _fetch_one_player(
    player: Dict[str, str],
    season: Optional[int],
    tour_code: str,
    filter_tournament_id: Optional[str] = None,
) -> List[PlayerTournamentScorecard]:
    """
    Fetch scorecard history for one player; return empty list on soft failure.

    Args:
        filter_tournament_id: When set, only return the matching tournament row.
    """
    pid = player["player_id"]
    name = player["player_name"]
    try:
        time.sleep(REQUEST_DELAY)
        rows = fetch_player_scorecard_history(
            player_id=pid,
            player_name=name,
            season=season,
            tour_code=tour_code,
        )
        if filter_tournament_id:
            rows = [r for r in rows if r.tournament_id == filter_tournament_id]
        return rows
    except Exception as exc:
        print(
            f"[player_scorecard] WARN: skipping player {pid} ({name}): {exc}",
            flush=True,
        )
        return []


# ---------------------------------------------------------------------------
# Core ingest
# ---------------------------------------------------------------------------


def _run_ingest(
    players: List[Dict[str, str]],
    season: Optional[int],
    tour_code: str,
    *,
    filter_tournament_id: Optional[str] = None,
    dry_run: bool = False,
    truncate: bool = False,
    run_ts: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Fetch scorecard history for all ``players`` and write to BigQuery.

    Returns a summary dict with counts.
    """
    ts = run_ts or datetime.datetime.utcnow().isoformat()

    client = _bq_client()
    table_id = _ensure_table(client, truncate=truncate)

    all_rows: List[PlayerTournamentScorecard] = []
    completed = 0
    total = len(players)

    label = (
        f"season={season or 'current'}"
        + (f" tournament={filter_tournament_id}" if filter_tournament_id else "")
    )
    print(
        f"[player_scorecard] Fetching scorecards for {total} players "
        f"({label}, workers={MAX_WORKERS}, delay={REQUEST_DELAY}s)…",
        flush=True,
    )

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(
                _fetch_one_player, p, season, tour_code, filter_tournament_id
            ): p
            for p in players
        }
        for future in as_completed(futures):
            rows = future.result()
            all_rows.extend(rows)
            completed += 1
            if completed % 25 == 0 or completed == total:
                print(
                    f"[player_scorecard] {completed}/{total} players done, "
                    f"{len(all_rows)} tournament rows collected so far.",
                    flush=True,
                )

    records = scorecard_history_to_records(all_rows, run_ts=ts)
    rows_inserted = 0

    if not dry_run and records:
        rows_inserted = _insert_rows(client, table_id, records)
        print(
            f"[player_scorecard] Inserted {rows_inserted} rows into {table_id}.",
            flush=True,
        )
    elif dry_run:
        print(
            f"[player_scorecard] DRY RUN — would insert {len(records)} rows.",
            flush=True,
        )

    return {
        "players_fetched": total,
        "tournament_rows_collected": len(all_rows),
        "rows_inserted": rows_inserted,
    }


# ---------------------------------------------------------------------------
# Public runners
# ---------------------------------------------------------------------------


def run_backfill(
    seasons: Optional[List[int]] = None,
    tour_code: str = "R",
    dry_run: bool = False,
    truncate_first: bool = True,
) -> Dict[str, Any]:
    """
    Backfill the website_player_scorecard table for the specified seasons.

    For each season, fetches every active player's complete tournament history
    and inserts it into BigQuery.

    Args:
        seasons:       List of season years to backfill.  Defaults to
                       ``PGA_START_SEASON..PGA_END_SEASON`` or the last
                       ``PGA_BACKFILL_YEARS`` seasons.
        tour_code:     Tour code (default ``"R"``).
        dry_run:       Fetch but skip BigQuery writes.
        truncate_first: Truncate the table before the first season's data.
    """
    if seasons is None:
        start = os.getenv("PGA_START_SEASON")
        end = os.getenv("PGA_END_SEASON")
        if start and end:
            seasons = list(range(int(start), int(end) + 1))
        else:
            years_back = int(os.getenv("PGA_BACKFILL_YEARS", "8"))
            current = datetime.datetime.utcnow().year
            seasons = [current - i for i in range(years_back)]

    summary: Dict[str, Any] = {
        "mode": "backfill",
        "seasons": seasons,
        "tour_code": tour_code,
        "season_results": [],
        "errors": [],
    }

    # Build the full player list once from website_active_players (BigQuery).
    # Fall back to the statOverview API if BQ is unavailable or empty.
    print(
        "[player_scorecard] Loading player list from website_active_players…",
        flush=True,
    )
    bq_players = _get_players_from_website_active_players()
    if bq_players:
        print(
            f"[player_scorecard] Found {len(bq_players)} players in website_active_players.",
            flush=True,
        )
    else:
        print(
            "[player_scorecard] website_active_players returned no players; "
            "will fall back to statOverview per season.",
            flush=True,
        )

    first = True
    for season in seasons:
        print(
            f"[player_scorecard] === Backfill season={season} ===",
            flush=True,
        )
        try:
            if bq_players:
                players = bq_players
            else:
                players = _get_active_players_from_stat_overview(
                    tour_code=tour_code, season=season
                )
            if not players:
                print(
                    f"[player_scorecard] No active players for season={season}, skipping.",
                    flush=True,
                )
                continue

            result = _run_ingest(
                players=players,
                season=season,
                tour_code=tour_code,
                dry_run=dry_run,
                truncate=(truncate_first and first),
            )
            result["season"] = season
            summary["season_results"].append(result)
            first = False
        except Exception as exc:
            msg = f"season={season} error={exc}"
            print(f"[player_scorecard] ERROR {msg}", flush=True)
            summary["errors"].append(msg)

    return summary


def run_daily(
    tour_code: str = "R",
    season: Optional[int] = None,
    tournament_id: Optional[str] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Daily update: load scorecard data for players who played in the most
    recently active tournament (the round completed yesterday or today).

    Args:
        tour_code:     Tour code (default ``"R"``).
        season:        Season year.  Defaults to the current calendar year.
        tournament_id: Override the target tournament.  When omitted the
                       most recently started tournament is auto-detected.
        dry_run:       Fetch but skip BigQuery writes.
    """
    effective_season = season or datetime.datetime.utcnow().year
    effective_tournament = tournament_id

    if effective_tournament is None:
        print(
            f"[player_scorecard] Detecting most recent tournament for "
            f"tour={tour_code} year={effective_season}…",
            flush=True,
        )
        effective_tournament = _get_most_recent_tournament_id(
            tour_code=tour_code, year=effective_season
        )

    if not effective_tournament:
        print(
            "[player_scorecard] No active tournament found — nothing to do.",
            flush=True,
        )
        return {
            "mode": "daily",
            "tournament_id": None,
            "players_fetched": 0,
            "rows_inserted": 0,
        }

    print(
        f"[player_scorecard] Daily update for tournament={effective_tournament}, "
        f"season={effective_season}",
        flush=True,
    )

    players = _get_recent_tournament_players(effective_tournament)
    if not players:
        print(
            f"[player_scorecard] No players found on leaderboard for "
            f"{effective_tournament} — nothing to do.",
            flush=True,
        )
        return {
            "mode": "daily",
            "tournament_id": effective_tournament,
            "players_fetched": 0,
            "rows_inserted": 0,
        }

    result = _run_ingest(
        players=players,
        season=effective_season,
        tour_code=tour_code,
        filter_tournament_id=effective_tournament,
        dry_run=dry_run,
        truncate=False,
    )
    result["mode"] = "daily"
    result["tournament_id"] = effective_tournament
    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Ingest PGA Tour player tournament scorecard history into BigQuery. "
            "Run with --backfill for historical load or --daily for nightly updates."
        )
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--backfill",
        action="store_true",
        help="Run historical backfill for all specified seasons.",
    )
    mode.add_argument(
        "--daily",
        action="store_true",
        help="Run daily update for the most recent active tournament.",
    )

    parser.add_argument(
        "--start-season",
        type=int,
        metavar="YEAR",
        help="First season to backfill (inclusive).  Overrides PGA_START_SEASON env var.",
    )
    parser.add_argument(
        "--end-season",
        type=int,
        metavar="YEAR",
        help="Last season to backfill (inclusive).  Overrides PGA_END_SEASON env var.",
    )
    parser.add_argument(
        "--years",
        type=int,
        metavar="N",
        help="Number of recent seasons to backfill (default 5).  Overrides PGA_BACKFILL_YEARS.",
    )
    parser.add_argument(
        "--season",
        type=int,
        metavar="YEAR",
        help="Season year for --daily (default: current year).",
    )
    parser.add_argument(
        "--tournament",
        metavar="TOURNAMENT_ID",
        help="Override tournament ID for --daily (e.g. R2026010).",
    )
    parser.add_argument(
        "--tour",
        default="R",
        metavar="TOUR_CODE",
        help="Tour code (default: R).",
    )
    parser.add_argument(
        "--no-truncate",
        action="store_true",
        help="Skip truncating the table before backfill (append mode).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch data but skip BigQuery writes.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        dest="as_json",
        help="Output summary as JSON.",
    )
    return parser


def _main() -> None:
    import json

    parser = _build_parser()
    args = parser.parse_args()

    # Apply CLI overrides to env vars so _run_ingest / run_backfill pick them up
    if args.years:
        os.environ["PGA_BACKFILL_YEARS"] = str(args.years)

    if args.backfill:
        seasons: Optional[List[int]] = None
        if args.start_season and args.end_season:
            seasons = list(range(args.start_season, args.end_season + 1))
        elif args.start_season:
            seasons = [args.start_season]

        summary = run_backfill(
            seasons=seasons,
            tour_code=args.tour,
            dry_run=args.dry_run,
            truncate_first=not args.no_truncate,
        )
    else:
        summary = run_daily(
            tour_code=args.tour,
            season=args.season,
            tournament_id=args.tournament,
            dry_run=args.dry_run,
        )

    if args.as_json:
        print(json.dumps(summary, indent=2, default=str))
    else:
        print("\n[player_scorecard] Done.")
        print(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    _main()

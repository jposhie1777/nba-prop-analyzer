"""
Ingest PGA Tour player scorecards (hole-by-hole) → BigQuery.

Fetches the PGA Tour schedule for a season to get tournament IDs, then pulls
the leaderboard for each tournament to get all players, then calls scorecardV3
for each player. Writes one row per hole per round to the ``player_scorecards``
BigQuery table.

This produces a LOT of API calls (~100-150 per tournament). For a full season
backfill leave plenty of time or use --tournament to ingest one tournament at a
time.

Usage (standalone CLI):
    # One full season (all tournaments)
    python -m mobile_api.ingest.pga.pga_scorecards_ingest --year 2025 --tour R

    # Single tournament (faster, good for daily/recent ingest)
    python -m mobile_api.ingest.pga.pga_scorecards_ingest --tournament R2026010

    # Dry-run (fetches but does not write to BQ)
    python -m mobile_api.ingest.pga.pga_scorecards_ingest --year 2025 --dry-run
"""

from __future__ import annotations

import argparse
import datetime
import os
import time
from typing import List, Optional

from google.cloud import bigquery

from .pga_leaderboard import fetch_leaderboard
from .pga_schedule import fetch_schedule
from .pga_scorecards import fetch_scorecard, scorecard_to_records

DATASET = os.getenv("PGA_DATASET", "pga_data")
TABLE = os.getenv("PGA_SCORECARDS_TABLE", "player_scorecards")
CHUNK_SIZE = 500
DELAY_BETWEEN_PLAYERS = float(os.getenv("PGA_SCORECARD_DELAY", "0.4"))

_ROUND_SCORES_SCHEMA = [
    bigquery.SchemaField("run_ts", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("season", "INT64"),
    bigquery.SchemaField("tournament_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("tournament_name", "STRING"),
    bigquery.SchemaField("tournament_start_date", "STRING"),
    bigquery.SchemaField("round_number", "INTEGER"),
    bigquery.SchemaField("player_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("player_display_name", "STRING"),
    bigquery.SchemaField("round_score", "INTEGER"),
    bigquery.SchemaField("par_relative_score", "INTEGER"),
    bigquery.SchemaField("total_score", "INTEGER"),
]

_SCHEMA = [
    bigquery.SchemaField("run_ts", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("tournament_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("player_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("player_display_name", "STRING"),
    bigquery.SchemaField("round_number", "INTEGER"),
    bigquery.SchemaField("round_par_relative_score", "INTEGER"),
    bigquery.SchemaField("round_strokes", "INTEGER"),
    bigquery.SchemaField("birdies", "INTEGER"),
    bigquery.SchemaField("bogeys", "INTEGER"),
    bigquery.SchemaField("eagles", "INTEGER"),
    bigquery.SchemaField("pars", "INTEGER"),
    bigquery.SchemaField("double_or_worse", "INTEGER"),
    bigquery.SchemaField("greens_in_regulation", "INTEGER"),
    bigquery.SchemaField("fairways_hit", "INTEGER"),
    bigquery.SchemaField("putts", "INTEGER"),
    bigquery.SchemaField("driving_distance", "INTEGER"),
    bigquery.SchemaField("driving_accuracy", "FLOAT64"),
    bigquery.SchemaField("hole_number", "INTEGER"),
    bigquery.SchemaField("par", "INTEGER"),
    bigquery.SchemaField("score", "INTEGER"),
    bigquery.SchemaField("birdie", "BOOL"),
    bigquery.SchemaField("eagle", "BOOL"),
    bigquery.SchemaField("bogey", "BOOL"),
    bigquery.SchemaField("double_or_worse_hole", "BOOL"),
    bigquery.SchemaField("hole_putts", "INTEGER"),
    bigquery.SchemaField("hole_driving_distance", "INTEGER"),
    bigquery.SchemaField("hole_in_one", "BOOL"),
]


def _bq_client() -> bigquery.Client:
    project = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
    return bigquery.Client(project=project)


def _ensure_table(client: bigquery.Client) -> str:
    bq_table = bigquery.Table(
        f"{client.project}.{DATASET}.{TABLE}",
        schema=_SCHEMA,
    )
    bq_table.clustering_fields = ["tournament_id", "player_id", "round_number"]
    bq_table.description = "PGA Tour hole-by-hole scorecards (scorecardV3 GraphQL)"
    client.create_table(bq_table, exists_ok=True)
    return f"{client.project}.{DATASET}.{TABLE}"


def _ensure_round_scores_table(client: bigquery.Client) -> str:
    bq_table = bigquery.Table(
        f"{client.project}.{DATASET}.tournament_round_scores",
        schema=_ROUND_SCORES_SCHEMA,
    )
    bq_table.clustering_fields = ["tournament_id", "player_id", "round_number"]
    client.create_table(bq_table, exists_ok=True)
    return f"{client.project}.{DATASET}.tournament_round_scores"


def _scorecard_to_round_scores(
    scorecard,
    *,
    run_ts: str,
    season: Optional[int] = None,
    tournament_name: Optional[str] = None,
    tournament_start_date: Optional[str] = None,
) -> list:
    """Extract per-round summary rows from a scorecard for tournament_round_scores."""
    rows = []
    cumulative = 0
    for rnd in scorecard.rounds:
        if rnd.strokes is not None:
            cumulative += rnd.strokes
        rows.append({
            "run_ts": run_ts,
            "ingested_at": run_ts,
            "season": season,
            "tournament_id": scorecard.tournament_id,
            "tournament_name": tournament_name,
            "tournament_start_date": tournament_start_date,
            "round_number": rnd.round_number,
            "player_id": scorecard.player_id,
            "player_display_name": scorecard.display_name,
            "round_score": rnd.strokes,
            "par_relative_score": rnd.par_relative_score,
            "total_score": cumulative if rnd.strokes is not None else None,
        })
    return rows


def _insert_rows(client: bigquery.Client, table_id: str, rows: list) -> int:
    if not rows:
        return 0
    inserted = 0
    for i in range(0, len(rows), CHUNK_SIZE):
        chunk = rows[i : i + CHUNK_SIZE]
        errors = client.insert_rows_json(table_id, chunk)
        if errors:
            raise RuntimeError(f"BigQuery insert errors: {errors[:3]}")
        inserted += len(chunk)
        time.sleep(0.05)
    return inserted


def ingest_tournament_scorecards(
    tournament_id: str,
    *,
    dry_run: bool = False,
    create_tables: bool = True,
    run_ts: Optional[str] = None,
    client: Optional[bigquery.Client] = None,
    table_id: Optional[str] = None,
    round_scores_table_id: Optional[str] = None,
    season: Optional[int] = None,
    tournament_name: Optional[str] = None,
    tournament_start_date: Optional[str] = None,
) -> dict:
    """
    Ingest scorecards for all players in a single tournament.

    Also writes per-round score summaries to ``tournament_round_scores``
    (round_score = actual strokes, par_relative_score, cumulative total_score).

    Args:
        tournament_id:         PGA Tour tournament ID string, e.g. ``"R2026010"``.
        dry_run:               Fetch but skip BigQuery writes.
        create_tables:         Auto-create BQ tables if missing.
        run_ts:                ISO timestamp for run_ts / ingested_at fields.
        client:                Reuse an existing BQ client.
        table_id:              Reuse a resolved scorecards table ID.
        round_scores_table_id: Reuse a resolved round_scores table ID.
        season:                Season year for round scores.
        tournament_name:       Tournament name for round scores denorm.
        tournament_start_date: Tournament start date for round scores denorm.

    Returns:
        Dict with ``tournament_id``, ``players_found``, ``rows_inserted``.
    """
    ts = run_ts or datetime.datetime.utcnow().isoformat()

    players = fetch_leaderboard(tournament_id)
    print(f"  [scorecards] {tournament_id}: {len(players)} players on leaderboard")

    if not players:
        return {"tournament_id": tournament_id, "players_found": 0, "rows_inserted": 0}

    if not dry_run:
        if client is None:
            client = _bq_client()
        if table_id is None:
            table_id = _ensure_table(client) if create_tables else f"{client.project}.{DATASET}.{TABLE}"
        if round_scores_table_id is None:
            round_scores_table_id = (
                _ensure_round_scores_table(client) if create_tables
                else f"{client.project}.{DATASET}.tournament_round_scores"
            )

    all_rows: list = []
    all_round_rows: list = []
    skipped = 0
    for player in players:
        try:
            scorecard = fetch_scorecard(tournament_id, player.player_id)
        except Exception as exc:
            print(f"    [scorecards] skip {player.player_id} ({player.display_name}): {exc}")
            skipped += 1
            time.sleep(DELAY_BETWEEN_PLAYERS)
            continue

        if scorecard:
            all_rows.extend(scorecard_to_records(scorecard, run_ts=ts))
            all_round_rows.extend(_scorecard_to_round_scores(
                scorecard,
                run_ts=ts,
                season=season,
                tournament_name=tournament_name,
                tournament_start_date=tournament_start_date,
            ))

        time.sleep(DELAY_BETWEEN_PLAYERS)

    inserted = 0
    if not dry_run:
        if all_rows:
            inserted = _insert_rows(client, table_id, all_rows)
        if all_round_rows:
            _insert_rows(client, round_scores_table_id, all_round_rows)

    print(f"  [scorecards] {tournament_id}: {len(all_rows)} hole rows, {len(all_round_rows)} round rows, {skipped} skipped")
    return {
        "tournament_id": tournament_id,
        "players_found": len(players),
        "rows_inserted": inserted,
    }


def ingest_season_scorecards(
    year: int,
    tour_code: str = "R",
    *,
    dry_run: bool = False,
    create_tables: bool = True,
    run_ts: Optional[str] = None,
) -> dict:
    """
    Ingest scorecards for all completed tournaments in a season.

    Uses the PGA Tour schedule to enumerate completed tournaments, then calls
    :func:`ingest_tournament_scorecards` for each one.
    """
    ts = run_ts or datetime.datetime.utcnow().isoformat()
    tournaments = fetch_schedule(tour_code=tour_code, year=str(year))
    completed = [t for t in tournaments if t.bucket == "completed"]

    print(f"[scorecards] {tour_code}/{year}: {len(completed)} completed tournaments")

    client = None
    table_id = None
    round_scores_table_id = None
    if not dry_run:
        client = _bq_client()
        table_id = _ensure_table(client) if create_tables else f"{client.project}.{DATASET}.{TABLE}"
        round_scores_table_id = (
            _ensure_round_scores_table(client) if create_tables
            else f"{client.project}.{DATASET}.tournament_round_scores"
        )

    total_inserted = 0
    for t in completed:
        result = ingest_tournament_scorecards(
            t.tournament_id,
            dry_run=dry_run,
            create_tables=False,
            run_ts=ts,
            client=client,
            table_id=table_id,
            round_scores_table_id=round_scores_table_id,
            season=year,
            tournament_name=t.name,
            tournament_start_date=t.start_date,
        )
        total_inserted += result["rows_inserted"]
        time.sleep(1.0)

    print(f"[scorecards] {tour_code}/{year}: {total_inserted} total rows inserted")
    return {"year": year, "tour_code": tour_code, "tournaments": len(completed), "rows_inserted": total_inserted}


def _cli() -> None:
    parser = argparse.ArgumentParser(description="Ingest PGA Tour scorecards to BigQuery.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--year", type=int, help="Season year (fetches all completed tournaments)")
    group.add_argument("--tournament", metavar="TOURNAMENT_ID", help="Single PGA Tour tournament ID, e.g. R2026010")
    parser.add_argument("--tour", default="R", metavar="TOUR_CODE")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-create-table", action="store_true")
    args = parser.parse_args()

    create_tables = not args.no_create_table

    if args.tournament:
        result = ingest_tournament_scorecards(
            args.tournament,
            dry_run=args.dry_run,
            create_tables=create_tables,
        )
    else:
        result = ingest_season_scorecards(
            args.year,
            tour_code=args.tour,
            dry_run=args.dry_run,
            create_tables=create_tables,
        )
    print(result)


if __name__ == "__main__":
    _cli()

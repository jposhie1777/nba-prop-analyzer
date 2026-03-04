from __future__ import annotations

import os
import time
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

from google.api_core.exceptions import Conflict, NotFound
from google.cloud import bigquery

from .pga_leaderboard import fetch_leaderboard, leaderboard_to_records
from .pga_pairings_ingest import ingest_pairings
from .pga_rankings_ingest import ingest_rankings
from .pga_schedule import fetch_schedule, schedule_to_records
from .pga_scorecards import fetch_scorecard, fetch_scorecard_stats, scorecard_to_records
from .pga_stats_ingest import ingest_stats

DATASET = os.getenv("PGA_DATASET", "pga_data")
DATASET_LOCATION = os.getenv("PGA_DATASET_LOCATION", "US")

SCHEDULE_TABLE = os.getenv("PGA_WEBSITE_SCHEDULE_TABLE", "website_schedule")
LEADERBOARD_TABLE = os.getenv("PGA_WEBSITE_LEADERBOARD_TABLE", "website_leaderboard")
SCORECARDS_TABLE = os.getenv("PGA_WEBSITE_SCORECARDS_TABLE", "website_scorecards")

RANKINGS_TABLE = os.getenv("PGA_RANKINGS_TABLE", "priority_rankings")
STATS_TABLE = os.getenv("PGA_STATS_TABLE", "player_stats")
PAIRINGS_TABLE = os.getenv("PGA_PAIRINGS_TABLE", "tournament_round_pairings")

SCHEMA_SCHEDULE = [
    bigquery.SchemaField("run_ts", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("tournament_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("bucket", "STRING"),
    bigquery.SchemaField("start_date", "DATE"),
    bigquery.SchemaField("city", "STRING"),
    bigquery.SchemaField("state", "STRING"),
    bigquery.SchemaField("country", "STRING"),
    bigquery.SchemaField("status_type", "STRING"),
    bigquery.SchemaField("purse", "STRING"),
    bigquery.SchemaField("champion", "STRING"),
]

SCHEMA_LEADERBOARD = [
    bigquery.SchemaField("run_ts", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("tournament_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("player_id", "STRING"),
    bigquery.SchemaField("player_display_name", "STRING"),
    bigquery.SchemaField("first_name", "STRING"),
    bigquery.SchemaField("last_name", "STRING"),
    bigquery.SchemaField("country", "STRING"),
    bigquery.SchemaField("amateur", "BOOL"),
    bigquery.SchemaField("position", "STRING"),
    bigquery.SchemaField("sort_order", "INT64"),
    bigquery.SchemaField("total", "STRING"),
    bigquery.SchemaField("total_sort", "INT64"),
    bigquery.SchemaField("total_strokes", "INT64"),
    bigquery.SchemaField("thru", "STRING"),
    bigquery.SchemaField("score", "STRING"),
    bigquery.SchemaField("current_round", "INT64"),
    bigquery.SchemaField("player_state", "STRING"),
    bigquery.SchemaField("round_status", "STRING"),
    bigquery.SchemaField("back_nine", "BOOL"),
    bigquery.SchemaField("movement_direction", "STRING"),
    bigquery.SchemaField("movement_amount", "INT64"),
    bigquery.SchemaField("round_number", "INT64"),
    bigquery.SchemaField("round_score", "INT64"),
]

SCHEMA_SCORECARDS = [
    bigquery.SchemaField("run_ts", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("tournament_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("player_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("player_display_name", "STRING"),
    bigquery.SchemaField("round_number", "INT64"),
    bigquery.SchemaField("round_par_relative_score", "INT64"),
    bigquery.SchemaField("round_strokes", "INT64"),
    bigquery.SchemaField("birdies", "INT64"),
    bigquery.SchemaField("bogeys", "INT64"),
    bigquery.SchemaField("eagles", "INT64"),
    bigquery.SchemaField("pars", "INT64"),
    bigquery.SchemaField("double_or_worse", "INT64"),
    bigquery.SchemaField("greens_in_regulation", "INT64"),
    bigquery.SchemaField("fairways_hit", "INT64"),
    bigquery.SchemaField("putts", "INT64"),
    bigquery.SchemaField("driving_distance", "FLOAT64"),
    bigquery.SchemaField("driving_accuracy", "FLOAT64"),
    bigquery.SchemaField("hole_number", "INT64"),
    bigquery.SchemaField("par", "INT64"),
    bigquery.SchemaField("score", "INT64"),
    bigquery.SchemaField("birdie", "BOOL"),
    bigquery.SchemaField("eagle", "BOOL"),
    bigquery.SchemaField("bogey", "BOOL"),
    bigquery.SchemaField("double_or_worse_hole", "BOOL"),
    bigquery.SchemaField("hole_putts", "INT64"),
    bigquery.SchemaField("hole_driving_distance", "FLOAT64"),
    bigquery.SchemaField("hole_in_one", "BOOL"),
]


def _bq_client() -> bigquery.Client:
    project = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
    return bigquery.Client(project=project)


def _table_id(client: bigquery.Client, table: str) -> str:
    parts = table.split(".")
    if len(parts) == 3:
        return table
    if len(parts) == 2:
        return f"{client.project}.{table}"
    return f"{client.project}.{DATASET}.{table}"


def ensure_dataset(client: bigquery.Client) -> None:
    dataset_id = f"{client.project}.{DATASET}"
    try:
        client.get_dataset(dataset_id)
    except NotFound:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = DATASET_LOCATION
        client.create_dataset(dataset)
    except Conflict:
        return


def ensure_table(client: bigquery.Client, table: str, schema: List[bigquery.SchemaField]) -> None:
    table_id = _table_id(client, table)
    try:
        client.get_table(table_id)
    except NotFound:
        client.create_table(bigquery.Table(table_id, schema=schema))
    except Conflict:
        return


def truncate_table(client: bigquery.Client, table: str) -> None:
    client.query(f"TRUNCATE TABLE `{_table_id(client, table)}`").result()


def insert_rows(client: bigquery.Client, table: str, rows: List[Dict[str, Any]], *, chunk_size: int = 500) -> int:
    if not rows:
        return 0
    table_id = _table_id(client, table)
    written = 0
    for idx in range(0, len(rows), chunk_size):
        chunk = rows[idx : idx + chunk_size]
        errors = client.insert_rows_json(table_id, chunk)
        if errors:
            raise RuntimeError(f"BigQuery insert errors for {table}: {errors[:3]}")
        written += len(chunk)
        time.sleep(0.05)
    return written


def _season_range() -> List[int]:
    start = os.getenv("PGA_START_SEASON")
    end = os.getenv("PGA_END_SEASON")
    if start and end:
        return list(range(int(start), int(end) + 1))
    years_back = int(os.getenv("PGA_BACKFILL_YEARS", "5"))
    current = datetime.utcnow().year
    return [current - offset for offset in range(years_back)]


def _tournament_ids(rows: Iterable[Dict[str, Any]]) -> List[str]:
    seen: set[str] = set()
    values: List[str] = []
    for row in rows:
        tid = str(row.get("tournament_id") or "").strip()
        if tid and tid not in seen:
            seen.add(tid)
            values.append(tid)
    return values


def run_website_ingestion(*, season: Optional[int] = None, create_tables: bool = True, truncate_first: bool = False) -> Dict[str, Any]:
    client = _bq_client()
    ensure_dataset(client)

    if create_tables:
        ensure_table(client, SCHEDULE_TABLE, SCHEMA_SCHEDULE)
        ensure_table(client, LEADERBOARD_TABLE, SCHEMA_LEADERBOARD)
        ensure_table(client, SCORECARDS_TABLE, SCHEMA_SCORECARDS)

    if truncate_first:
        for table in [SCHEDULE_TABLE, LEADERBOARD_TABLE, SCORECARDS_TABLE, RANKINGS_TABLE, STATS_TABLE, PAIRINGS_TABLE]:
            try:
                truncate_table(client, table)
            except Exception:
                # table may not exist if create_tables=False for external tables
                pass

    seasons = [season] if season else _season_range()
    summary: Dict[str, Any] = {
        "mode": "website_only",
        "seasons": seasons,
        "schedule_rows": 0,
        "leaderboard_rows": 0,
        "scorecard_rows": 0,
        "pairings_rows": 0,
        "stats_rows": 0,
        "rankings_rows": 0,
        "errors": [],
    }

    for yr in seasons:
        print(f"[website] season={yr}: fetching schedule")
        tournaments = fetch_schedule(tour_code="R", year=str(yr))
        schedule_rows = schedule_to_records(tournaments)
        summary["schedule_rows"] += insert_rows(client, SCHEDULE_TABLE, schedule_rows)

        # website stats/rankings ingests (already writes to BQ)
        stats = ingest_stats(year=yr, tour_code="R", dry_run=False, create_tables=create_tables)
        rankings = ingest_rankings(year=yr, tour_code="R", dry_run=False, create_tables=create_tables)
        summary["stats_rows"] += int(stats.get("rows_inserted", 0))
        summary["rankings_rows"] += int(rankings.get("rows_inserted", 0))

        tournament_ids = _tournament_ids(schedule_rows)
        print(f"[website] season={yr}: tournaments={len(tournament_ids)}")

        for tournament_id in tournament_ids:
            players = []
            try:
                players = fetch_leaderboard(tournament_id)
                lb_rows = leaderboard_to_records(tournament_id, players)
                summary["leaderboard_rows"] += insert_rows(client, LEADERBOARD_TABLE, lb_rows)
            except Exception as exc:
                message = f"tournament={tournament_id} leaderboard error={exc}"
                print(f"[website] WARN {message}")
                summary["errors"].append(message)

            try:
                pairings_summary = ingest_pairings(
                    tournament_id=tournament_id,
                    round_number=0,
                    create_tables=create_tables,
                    dry_run=False,
                )
                summary["pairings_rows"] += int(pairings_summary.get("inserted", 0))
            except Exception as exc:
                message = f"tournament={tournament_id} pairings error={exc}"
                print(f"[website] WARN {message}")
                summary["errors"].append(message)

            player_ids = sorted({str(p.player_id) for p in players if getattr(p, "player_id", None)})
            for player_id in player_ids:
                try:
                    card = fetch_scorecard(tournament_id, player_id)
                    if card is None:
                        card = fetch_scorecard_stats(tournament_id, player_id)
                    if card is None:
                        continue
                    sc_rows = scorecard_to_records(card)
                    summary["scorecard_rows"] += insert_rows(client, SCORECARDS_TABLE, sc_rows)
                except Exception as exc:
                    message = f"tournament={tournament_id} player={player_id} scorecard error={exc}"
                    print(f"[website] WARN {message}")
                    summary["errors"].append(message)

    return summary


def run_website_backfill() -> Dict[str, Any]:
    return run_website_ingestion(season=None, create_tables=True, truncate_first=True)

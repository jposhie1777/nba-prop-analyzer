from __future__ import annotations

import os
import time
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional

from google.api_core.exceptions import Conflict, NotFound
from google.cloud import bigquery

from .pga_leaderboard import fetch_leaderboard, leaderboard_to_records
from .pga_pairings_ingest import ingest_pairings
from .pga_rankings_ingest import ingest_rankings
from .pga_schedule import fetch_schedule, schedule_to_records
from .pga_stats_ingest import ingest_website_player_stats

DATASET = os.getenv("PGA_DATASET", "pga_data")
DATASET_LOCATION = os.getenv("PGA_DATASET_LOCATION", "US")

SCHEDULE_TABLE = os.getenv("PGA_WEBSITE_SCHEDULE_TABLE", "website_schedule")
LEADERBOARD_TABLE = os.getenv("PGA_WEBSITE_LEADERBOARD_TABLE", "website_leaderboard")
SCORECARDS_TABLE = os.getenv("PGA_WEBSITE_SCORECARDS_TABLE", "website_scorecards")

RANKINGS_TABLE = os.getenv("PGA_RANKINGS_TABLE", "priority_rankings")
WEBSITE_PLAYER_STATS_TABLE = os.getenv("PGA_WEBSITE_PLAYER_STATS_TABLE", "website_player_stats")
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


def prune_table_to_tournament(client: bigquery.Client, table: str, tournament_id: str) -> int:
    """Delete rows for all tournaments except the selected tournament."""
    table_id = _table_id(client, table)
    query = f"DELETE FROM `{table_id}` WHERE tournament_id != @tournament_id"
    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("tournament_id", "STRING", tournament_id)
            ]
        ),
    )
    job.result()
    return int(getattr(job, "num_dml_affected_rows", 0) or 0)


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


def _parse_date(value: Any) -> Optional[date]:
    if not value:
        return None
    if isinstance(value, date):
        return value
    try:
        return datetime.fromisoformat(str(value)).date()
    except (TypeError, ValueError):
        return None


def _focused_tournament_ids(schedule_rows: List[Dict[str, Any]]) -> List[str]:
    today = datetime.utcnow().date()
    upcoming: List[tuple[date, str]] = []
    completed: List[tuple[date, str]] = []

    for row in schedule_rows:
        tournament_id = str(row.get("tournament_id") or "").strip()
        if not tournament_id:
            continue
        start_date = _parse_date(row.get("start_date"))
        if not start_date:
            continue
        bucket = str(row.get("bucket") or "").strip().lower()
        if bucket == "upcoming" and start_date <= today:
            upcoming.append((start_date, tournament_id))
        elif bucket == "completed":
            completed.append((start_date, tournament_id))

    if upcoming:
        return [max(upcoming, key=lambda item: item[0])[1]]
    if completed:
        return [max(completed, key=lambda item: item[0])[1]]
    return _tournament_ids(schedule_rows)[:1]


def _has_started_upcoming_tournament(schedule_rows: List[Dict[str, Any]], *, today: Optional[date] = None) -> bool:
    check_date = today or datetime.utcnow().date()
    for row in schedule_rows:
        start_date = _parse_date(row.get("start_date"))
        if not start_date:
            continue
        bucket = str(row.get("bucket") or "").strip().lower()
        if bucket == "upcoming" and start_date <= check_date:
            return True
    return False


def _maybe_weekly_truncate_pairings_table(
    client: bigquery.Client,
    schedule_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Truncate pairings table once per week on a configured weekday when idle.

    Intended for daily ingestion runs: clear stale previous-week pairings before
    new Wednesday/Thursday publication windows when no event is in progress.
    """
    enabled = os.getenv("PGA_PAIRINGS_WEEKLY_TRUNCATE", "true").strip().lower() == "true"
    day_name = os.getenv("PGA_PAIRINGS_WEEKLY_TRUNCATE_DAY", "wednesday").strip().lower()
    target_days = {
        "monday": 0,
        "tuesday": 1,
        "wednesday": 2,
        "thursday": 3,
        "friday": 4,
        "saturday": 5,
        "sunday": 6,
    }
    target_weekday = target_days.get(day_name, 2)

    today = datetime.utcnow().date()
    details: Dict[str, Any] = {
        "enabled": enabled,
        "today_weekday": today.weekday(),
        "target_weekday": target_weekday,
        "truncated": False,
        "reason": None,
    }
    if not enabled:
        details["reason"] = "disabled"
        return details

    if today.weekday() != target_weekday:
        details["reason"] = "not_scheduled_day"
        return details

    if _has_started_upcoming_tournament(schedule_rows, today=today):
        details["reason"] = "active_tournament_detected"
        return details

    table_id = _table_id(client, PAIRINGS_TABLE)
    lower_bound = datetime.utcnow() - timedelta(days=6)
    recent_query = f"""
    SELECT MAX(run_ts) AS latest_run_ts
    FROM `{table_id}`
    WHERE run_ts >= @lower_bound
    """
    try:
        result = list(
            client.query(
                recent_query,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("lower_bound", "TIMESTAMP", lower_bound)
                    ]
                ),
            ).result()
        )
        latest = result[0].get("latest_run_ts") if result else None
        if latest is not None:
            details["reason"] = "already_has_recent_rows"
            return details
    except Exception as exc:
        details["reason"] = f"recent_row_check_failed: {exc}"
        return details

    truncate_table(client, PAIRINGS_TABLE)
    details["truncated"] = True
    details["reason"] = "weekly_reset"
    return details


def _round_score_rows_from_leaderboard(
    tournament_id: str,
    leaderboard_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for row in leaderboard_rows:
        round_number = row.get("round_number")
        round_score = row.get("round_score")
        if round_number is None or round_score is None:
            continue
        rows.append(
            {
                "run_ts": row.get("run_ts"),
                "ingested_at": row.get("ingested_at"),
                "tournament_id": tournament_id,
                "player_id": row.get("player_id"),
                "player_display_name": row.get("player_display_name"),
                "round_number": round_number,
                "round_par_relative_score": round_score,
            }
        )
    return rows


def run_website_ingestion(*, season: Optional[int] = None, create_tables: bool = True, truncate_first: bool = False, weekly_pairings_truncate: bool = False) -> Dict[str, Any]:
    client = _bq_client()
    ensure_dataset(client)

    if create_tables:
        ensure_table(client, SCHEDULE_TABLE, SCHEMA_SCHEDULE)
        ensure_table(client, LEADERBOARD_TABLE, SCHEMA_LEADERBOARD)
        ensure_table(client, SCORECARDS_TABLE, SCHEMA_SCORECARDS)

    if truncate_first:
        for table in [SCHEDULE_TABLE, LEADERBOARD_TABLE, SCORECARDS_TABLE, RANKINGS_TABLE, WEBSITE_PLAYER_STATS_TABLE, PAIRINGS_TABLE]:
            try:
                truncate_table(client, table)
            except Exception:
                # table may not exist if create_tables=False for external tables
                pass

    if season is not None:
        seasons = [season]
    elif truncate_first:
        seasons = _season_range()
    else:
        seasons = [datetime.utcnow().year]
    summary: Dict[str, Any] = {
        "mode": "website_only",
        "seasons": seasons,
        "schedule_rows": 0,
        "leaderboard_rows": 0,
        "scorecard_rows": 0,
        "pairings_rows": 0,
        "website_player_stats_rows": 0,
        "rankings_rows": 0,
        "errors": [],
        "pairings_truncate": {"enabled": bool(weekly_pairings_truncate), "truncated": False, "reason": None},
        "leaderboard_pruned_other_tournaments": 0,
        "scorecards_pruned_other_tournaments": 0,
    }

    for yr in seasons:
        print(f"[website] season={yr}: fetching schedule")
        tournaments = fetch_schedule(tour_code="R", year=str(yr))
        schedule_rows = schedule_to_records(tournaments)
        summary["schedule_rows"] += insert_rows(client, SCHEDULE_TABLE, schedule_rows)

        if weekly_pairings_truncate:
            truncate_summary = _maybe_weekly_truncate_pairings_table(client, schedule_rows)
            summary["pairings_truncate"] = truncate_summary
            if truncate_summary.get("truncated"):
                print("[website] pairings table weekly truncate complete")

        # website stats/rankings ingests (already writes to BQ)
        website_stats = ingest_website_player_stats(year=yr, tour_code="R", dry_run=False, create_tables=create_tables)
        rankings = ingest_rankings(year=yr, tour_code="R", dry_run=False, create_tables=create_tables)
        summary["website_player_stats_rows"] += int(website_stats.get("rows_inserted", 0))
        summary["rankings_rows"] += int(rankings.get("rows_inserted", 0))

        tournament_ids = _focused_tournament_ids(schedule_rows)
        print(f"[website] season={yr}: focused_tournaments={len(tournament_ids)}")

        for tournament_id in tournament_ids:
            players = []
            try:
                players = fetch_leaderboard(tournament_id)
                lb_rows = leaderboard_to_records(tournament_id, players)

                # Keep leaderboard/scorecard tables focused on current tournament.
                summary["leaderboard_pruned_other_tournaments"] += prune_table_to_tournament(
                    client, LEADERBOARD_TABLE, tournament_id
                )
                summary["scorecards_pruned_other_tournaments"] += prune_table_to_tournament(
                    client, SCORECARDS_TABLE, tournament_id
                )

                summary["leaderboard_rows"] += insert_rows(client, LEADERBOARD_TABLE, lb_rows)
                score_rows = _round_score_rows_from_leaderboard(tournament_id, lb_rows)
                if score_rows:
                    summary["scorecard_rows"] += insert_rows(client, SCORECARDS_TABLE, score_rows)
            except Exception as exc:
                message = f"tournament={tournament_id} leaderboard error={exc}"
                print(f"[website] WARN {message}")
                summary["errors"].append(message)

            # Keep pairings table focused to the current event only.
            if yr != datetime.utcnow().year:
                continue

            try:
                pairings_summary = ingest_pairings(
                    tournament_id=tournament_id,
                    round_number=0,
                    create_tables=create_tables,
                    dry_run=False,
                    keep_only_tournament=True,
                )
                summary["pairings_rows"] += int(pairings_summary.get("inserted", 0))
            except Exception as exc:
                message = f"tournament={tournament_id} pairings error={exc}"
                print(f"[website] WARN {message}")
                summary["errors"].append(message)

    return summary


def run_daily_ingestion(*, season: Optional[int] = None, create_tables: bool = True) -> Dict[str, Any]:
    """Daily PGA website ingest with optional weekly pairings table reset."""
    return run_website_ingestion(
        season=season,
        create_tables=create_tables,
        truncate_first=False,
        weekly_pairings_truncate=True,
    )


def run_website_backfill() -> Dict[str, Any]:
    return run_website_ingestion(season=None, create_tables=True, truncate_first=True, weekly_pairings_truncate=False)

from __future__ import annotations

import os
import time
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

from google.cloud import bigquery

from pga.client import PgaApiError, fetch_paginated


DATASET = os.getenv("PGA_DATASET", "pga_data")
PROJECT = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
LOCATION = os.getenv("PGA_DATASET_LOCATION", "US")


def get_client() -> bigquery.Client:
    if PROJECT:
        return bigquery.Client(project=PROJECT)
    return bigquery.Client()


def ensure_dataset(client: bigquery.Client) -> None:
    dataset_id = f"{client.project}.{DATASET}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = LOCATION
    client.create_dataset(dataset, exists_ok=True)


def create_table(
    client: bigquery.Client,
    table_name: str,
    schema: List[bigquery.SchemaField],
    *,
    partition_field: Optional[str] = None,
    cluster_fields: Optional[List[str]] = None,
) -> None:
    table_id = f"{client.project}.{DATASET}.{table_name}"
    table = bigquery.Table(table_id, schema=schema)
    if partition_field:
        table.range_partitioning = bigquery.RangePartitioning(
            field=partition_field,
            range_=bigquery.PartitionRange(start=2015, end=2035, interval=1),
        )
    if cluster_fields:
        table.clustering_fields = cluster_fields
    client.create_table(table, exists_ok=True)


def ensure_tables(client: bigquery.Client) -> None:
    create_table(
        client,
        "players",
        [
            bigquery.SchemaField("run_ts", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("player_id", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("first_name", "STRING"),
            bigquery.SchemaField("last_name", "STRING"),
            bigquery.SchemaField("display_name", "STRING"),
            bigquery.SchemaField("country", "STRING"),
            bigquery.SchemaField("country_code", "STRING"),
            bigquery.SchemaField("height", "STRING"),
            bigquery.SchemaField("weight", "STRING"),
            bigquery.SchemaField("birth_date", "STRING"),
            bigquery.SchemaField("birthplace_city", "STRING"),
            bigquery.SchemaField("birthplace_state", "STRING"),
            bigquery.SchemaField("birthplace_country", "STRING"),
            bigquery.SchemaField("turned_pro", "STRING"),
            bigquery.SchemaField("school", "STRING"),
            bigquery.SchemaField("residence_city", "STRING"),
            bigquery.SchemaField("residence_state", "STRING"),
            bigquery.SchemaField("residence_country", "STRING"),
            bigquery.SchemaField("owgr", "INT64"),
            bigquery.SchemaField("active", "BOOL"),
        ],
        cluster_fields=["player_id"],
    )

    create_table(
        client,
        "courses",
        [
            bigquery.SchemaField("run_ts", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("course_id", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("city", "STRING"),
            bigquery.SchemaField("state", "STRING"),
            bigquery.SchemaField("country", "STRING"),
            bigquery.SchemaField("par", "INT64"),
            bigquery.SchemaField("yardage", "STRING"),
            bigquery.SchemaField("established", "STRING"),
            bigquery.SchemaField("architect", "STRING"),
            bigquery.SchemaField("fairway_grass", "STRING"),
            bigquery.SchemaField("rough_grass", "STRING"),
            bigquery.SchemaField("green_grass", "STRING"),
        ],
        cluster_fields=["course_id"],
    )

    create_table(
        client,
        "tournaments",
        [
            bigquery.SchemaField("run_ts", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("tournament_id", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("season", "INT64"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("start_date", "TIMESTAMP"),
            bigquery.SchemaField("end_date", "STRING"),
            bigquery.SchemaField("city", "STRING"),
            bigquery.SchemaField("state", "STRING"),
            bigquery.SchemaField("country", "STRING"),
            bigquery.SchemaField("course_name", "STRING"),
            bigquery.SchemaField("purse", "STRING"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("champion_id", "INT64"),
            bigquery.SchemaField("champion_display_name", "STRING"),
            bigquery.SchemaField("champion_country", "STRING"),
            bigquery.SchemaField("courses", "JSON"),
        ],
        partition_field="season",
        cluster_fields=["tournament_id", "season"],
    )

    create_table(
        client,
        "tournament_results",
        [
            bigquery.SchemaField("run_ts", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("season", "INT64"),
            bigquery.SchemaField("tournament_id", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("tournament_name", "STRING"),
            bigquery.SchemaField("tournament_start_date", "TIMESTAMP"),
            bigquery.SchemaField("player_id", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("player_display_name", "STRING"),
            bigquery.SchemaField("position", "STRING"),
            bigquery.SchemaField("position_numeric", "INT64"),
            bigquery.SchemaField("total_score", "INT64"),
            bigquery.SchemaField("par_relative_score", "INT64"),
        ],
        partition_field="season",
        cluster_fields=["tournament_id", "player_id"],
    )

    create_table(
        client,
        "tournament_course_stats",
        [
            bigquery.SchemaField("run_ts", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("season", "INT64"),
            bigquery.SchemaField("tournament_id", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("tournament_name", "STRING"),
            bigquery.SchemaField("course_id", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("course_name", "STRING"),
            bigquery.SchemaField("hole_number", "INT64"),
            bigquery.SchemaField("round_number", "INT64"),
            bigquery.SchemaField("scoring_average", "FLOAT64"),
            bigquery.SchemaField("scoring_diff", "FLOAT64"),
            bigquery.SchemaField("difficulty_rank", "INT64"),
            bigquery.SchemaField("eagles", "INT64"),
            bigquery.SchemaField("birdies", "INT64"),
            bigquery.SchemaField("pars", "INT64"),
            bigquery.SchemaField("bogeys", "INT64"),
            bigquery.SchemaField("double_bogeys", "INT64"),
        ],
        partition_field="season",
        cluster_fields=["tournament_id", "course_id"],
    )

    create_table(
        client,
        "course_holes",
        [
            bigquery.SchemaField("run_ts", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("course_id", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("course_name", "STRING"),
            bigquery.SchemaField("hole_number", "INT64"),
            bigquery.SchemaField("par", "INT64"),
            bigquery.SchemaField("yardage", "INT64"),
        ],
        cluster_fields=["course_id"],
    )


def insert_rows(
    client: bigquery.Client,
    table: str,
    rows: List[Dict[str, Any]],
    *,
    chunk_size: int = 500,
) -> None:
    if not rows:
        return
    table_id = f"{client.project}.{DATASET}.{table}"
    for idx in range(0, len(rows), chunk_size):
        chunk = rows[idx : idx + chunk_size]
        errors = client.insert_rows_json(table_id, chunk)
        if errors:
            raise RuntimeError(f"BigQuery insert errors for {table}: {errors}")
        time.sleep(0.05)


def _fetch_paginated_retry(
    path: str,
    params: Optional[Dict[str, Any]] = None,
    *,
    per_page: int = 100,
    max_pages: int = 200,
) -> List[Dict[str, Any]]:
    backoff = 5
    for attempt in range(5):
        try:
            return fetch_paginated(
                path,
                params=params or {},
                per_page=per_page,
                max_pages=max_pages,
                cache_ttl=0,
            )
        except PgaApiError as exc:
            message = str(exc)
            if "429" in message or "503" in message:
                time.sleep(backoff)
                backoff *= 2
                continue
            raise
    raise RuntimeError(f"Exceeded retries for {path}")


def _season_range() -> List[int]:
    start = os.getenv("PGA_START_SEASON")
    end = os.getenv("PGA_END_SEASON")
    if start and end:
        return list(range(int(start), int(end) + 1))
    years_back = int(os.getenv("PGA_BACKFILL_YEARS", "5"))
    current = datetime.utcnow().year
    return [current - offset for offset in range(years_back)]


def normalize_players(players: Iterable[Dict[str, Any]], run_ts: str) -> List[Dict[str, Any]]:
    rows = []
    for player in players:
        player_id = player.get("id")
        if not player_id:
            continue
        rows.append(
            {
                "run_ts": run_ts,
                "ingested_at": run_ts,
                "player_id": player_id,
                "first_name": player.get("first_name"),
                "last_name": player.get("last_name"),
                "display_name": player.get("display_name"),
                "country": player.get("country"),
                "country_code": player.get("country_code"),
                "height": player.get("height"),
                "weight": player.get("weight"),
                "birth_date": player.get("birth_date"),
                "birthplace_city": player.get("birthplace_city"),
                "birthplace_state": player.get("birthplace_state"),
                "birthplace_country": player.get("birthplace_country"),
                "turned_pro": player.get("turned_pro"),
                "school": player.get("school"),
                "residence_city": player.get("residence_city"),
                "residence_state": player.get("residence_state"),
                "residence_country": player.get("residence_country"),
                "owgr": player.get("owgr"),
                "active": player.get("active"),
            }
        )
    return rows


def normalize_courses(courses: Iterable[Dict[str, Any]], run_ts: str) -> List[Dict[str, Any]]:
    rows = []
    for course in courses:
        course_id = course.get("id")
        if not course_id:
            continue
        rows.append(
            {
                "run_ts": run_ts,
                "ingested_at": run_ts,
                "course_id": course_id,
                "name": course.get("name"),
                "city": course.get("city"),
                "state": course.get("state"),
                "country": course.get("country"),
                "par": course.get("par"),
                "yardage": course.get("yardage"),
                "established": course.get("established"),
                "architect": course.get("architect"),
                "fairway_grass": course.get("fairway_grass"),
                "rough_grass": course.get("rough_grass"),
                "green_grass": course.get("green_grass"),
            }
        )
    return rows


def normalize_tournaments(
    tournaments: Iterable[Dict[str, Any]],
    run_ts: str,
) -> List[Dict[str, Any]]:
    rows = []
    for tournament in tournaments:
        tournament_id = tournament.get("id")
        if not tournament_id:
            continue
        champion = tournament.get("champion") or {}
        rows.append(
            {
                "run_ts": run_ts,
                "ingested_at": run_ts,
                "tournament_id": tournament_id,
                "season": tournament.get("season"),
                "name": tournament.get("name"),
                "start_date": tournament.get("start_date"),
                "end_date": tournament.get("end_date"),
                "city": tournament.get("city"),
                "state": tournament.get("state"),
                "country": tournament.get("country"),
                "course_name": tournament.get("course_name"),
                "purse": tournament.get("purse"),
                "status": tournament.get("status"),
                "champion_id": champion.get("id"),
                "champion_display_name": champion.get("display_name"),
                "champion_country": champion.get("country"),
                "courses": tournament.get("courses"),
            }
        )
    return rows


def normalize_tournament_results(
    results: Iterable[Dict[str, Any]],
    run_ts: str,
) -> List[Dict[str, Any]]:
    rows = []
    for row in results:
        tournament = row.get("tournament") or {}
        player = row.get("player") or {}
        tournament_id = tournament.get("id")
        player_id = player.get("id")
        if not tournament_id or not player_id:
            continue
        rows.append(
            {
                "run_ts": run_ts,
                "ingested_at": run_ts,
                "season": tournament.get("season"),
                "tournament_id": tournament_id,
                "tournament_name": tournament.get("name"),
                "tournament_start_date": tournament.get("start_date"),
                "player_id": player_id,
                "player_display_name": player.get("display_name"),
                "position": row.get("position"),
                "position_numeric": row.get("position_numeric"),
                "total_score": row.get("total_score"),
                "par_relative_score": row.get("par_relative_score"),
            }
        )
    return rows


def normalize_course_stats(
    stats: Iterable[Dict[str, Any]],
    run_ts: str,
) -> List[Dict[str, Any]]:
    rows = []
    for row in stats:
        tournament = row.get("tournament") or {}
        course = row.get("course") or {}
        tournament_id = tournament.get("id")
        course_id = course.get("id")
        if not tournament_id or not course_id:
            continue
        rows.append(
            {
                "run_ts": run_ts,
                "ingested_at": run_ts,
                "season": tournament.get("season"),
                "tournament_id": tournament_id,
                "tournament_name": tournament.get("name"),
                "course_id": course_id,
                "course_name": course.get("name"),
                "hole_number": row.get("hole_number"),
                "round_number": row.get("round_number"),
                "scoring_average": row.get("scoring_average"),
                "scoring_diff": row.get("scoring_diff"),
                "difficulty_rank": row.get("difficulty_rank"),
                "eagles": row.get("eagles"),
                "birdies": row.get("birdies"),
                "pars": row.get("pars"),
                "bogeys": row.get("bogeys"),
                "double_bogeys": row.get("double_bogeys"),
            }
        )
    return rows


def normalize_course_holes(
    holes: Iterable[Dict[str, Any]],
    run_ts: str,
) -> List[Dict[str, Any]]:
    rows = []
    for row in holes:
        course = row.get("course") or {}
        course_id = course.get("id")
        if not course_id:
            continue
        rows.append(
            {
                "run_ts": run_ts,
                "ingested_at": run_ts,
                "course_id": course_id,
                "course_name": course.get("name"),
                "hole_number": row.get("hole_number"),
                "par": row.get("par"),
                "yardage": row.get("yardage"),
            }
        )
    return rows


def truncate_table(client: bigquery.Client, table: str) -> None:
    table_id = f"{client.project}.{DATASET}.{table}"
    client.query(f"TRUNCATE TABLE `{table_id}`").result()


def main() -> None:
    client = get_client()
    ensure_dataset(client)
    ensure_tables(client)

    if os.getenv("PGA_TRUNCATE", "true").lower() == "true":
        for table in [
            "players",
            "courses",
            "tournaments",
            "tournament_results",
            "tournament_course_stats",
            "course_holes",
        ]:
            truncate_table(client, table)

    run_ts = datetime.utcnow().isoformat()

    print("Fetching players...")
    players = _fetch_paginated_retry("/players", per_page=100, max_pages=20)
    insert_rows(client, "players", normalize_players(players, run_ts))

    print("Fetching courses...")
    courses = _fetch_paginated_retry("/courses", per_page=100, max_pages=20)
    insert_rows(client, "courses", normalize_courses(courses, run_ts))

    print("Fetching course holes...")
    holes = _fetch_paginated_retry("/course_holes", per_page=100, max_pages=200)
    insert_rows(client, "course_holes", normalize_course_holes(holes, run_ts))

    seasons = _season_range()
    for season in seasons:
        print(f"Fetching tournaments for season {season}...")
        tournaments = _fetch_paginated_retry(
            "/tournaments",
            params={"season": season},
            per_page=100,
            max_pages=50,
        )
        insert_rows(client, "tournaments", normalize_tournaments(tournaments, run_ts))
        time.sleep(1.1)

        print(f"Fetching tournament results for season {season}...")
        results = _fetch_paginated_retry(
            "/tournament_results",
            params={"season": season},
            per_page=100,
            max_pages=500,
        )
        insert_rows(client, "tournament_results", normalize_tournament_results(results, run_ts))
        time.sleep(1.1)

        print(f"Fetching course stats for season {season}...")
        course_stats = _fetch_paginated_retry(
            "/tournament_course_stats",
            params={"season": season},
            per_page=100,
            max_pages=200,
        )
        insert_rows(client, "tournament_course_stats", normalize_course_stats(course_stats, run_ts))
        time.sleep(1.1)

    print("PGA backfill complete.")


if __name__ == "__main__":
    main()

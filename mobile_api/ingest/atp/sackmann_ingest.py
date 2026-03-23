from __future__ import annotations

import argparse
import csv
import io
import json
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

import requests
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

try:
    from bq import get_bq_client
except ModuleNotFoundError:
    from mobile_api.bq import get_bq_client


DEFAULT_DATASET = os.getenv("ATP_DATASET", "atp_data")
DEFAULT_LOCATION = os.getenv("ATP_BQ_LOCATION", "US")
SACKMANN_RAW_BASE = os.getenv(
    "SACKMANN_RAW_BASE",
    "https://raw.githubusercontent.com/JeffSackmann/tennis_atp/master",
)
SACKMANN_SOURCE_REPO = os.getenv("SACKMANN_SOURCE_REPO", "JeffSackmann/tennis_atp")
REQUEST_TIMEOUT_SECONDS = int(os.getenv("SACKMANN_REQUEST_TIMEOUT_SECONDS", "60"))

SACKMANN_RAW_TABLE = os.getenv(
    "ATP_SACKMANN_RAW_TABLE",
    f"{DEFAULT_DATASET}.sackmann_matches_raw",
)
SACKMANN_PLAYER_MATCH_TABLE = os.getenv(
    "ATP_SACKMANN_PLAYER_MATCH_TABLE",
    f"{DEFAULT_DATASET}.sackmann_player_match_stats",
)
SACKMANN_PLAYER_SURFACE_FEATURES_TABLE = os.getenv(
    "ATP_SACKMANN_PLAYER_SURFACE_FEATURES_TABLE",
    f"{DEFAULT_DATASET}.sackmann_player_surface_features",
)
SACKMANN_H2H_FEATURES_TABLE = os.getenv(
    "ATP_SACKMANN_H2H_FEATURES_TABLE",
    f"{DEFAULT_DATASET}.sackmann_h2h_features",
)

_SCORE_RE = re.compile(r"(\d+)-(\d+)")


RAW_SCHEMA = [
    bigquery.SchemaField("match_uid", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("season", "INT64"),
    bigquery.SchemaField("source_level", "STRING"),
    bigquery.SchemaField("source_url", "STRING"),
    bigquery.SchemaField("source_repo", "STRING"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    bigquery.SchemaField("tourney_id", "STRING"),
    bigquery.SchemaField("tourney_name", "STRING"),
    bigquery.SchemaField("surface", "STRING"),
    bigquery.SchemaField("draw_size", "INT64"),
    bigquery.SchemaField("tourney_level", "STRING"),
    bigquery.SchemaField("tourney_date", "DATE"),
    bigquery.SchemaField("match_num", "INT64"),
    bigquery.SchemaField("best_of", "INT64"),
    bigquery.SchemaField("round", "STRING"),
    bigquery.SchemaField("minutes", "INT64"),
    bigquery.SchemaField("winner_id", "INT64"),
    bigquery.SchemaField("winner_name", "STRING"),
    bigquery.SchemaField("winner_ioc", "STRING"),
    bigquery.SchemaField("winner_age", "FLOAT64"),
    bigquery.SchemaField("loser_id", "INT64"),
    bigquery.SchemaField("loser_name", "STRING"),
    bigquery.SchemaField("loser_ioc", "STRING"),
    bigquery.SchemaField("loser_age", "FLOAT64"),
    bigquery.SchemaField("score", "STRING"),
    bigquery.SchemaField("w_ace", "INT64"),
    bigquery.SchemaField("w_df", "INT64"),
    bigquery.SchemaField("l_ace", "INT64"),
    bigquery.SchemaField("l_df", "INT64"),
    bigquery.SchemaField("winner_rank", "INT64"),
    bigquery.SchemaField("winner_rank_points", "INT64"),
    bigquery.SchemaField("loser_rank", "INT64"),
    bigquery.SchemaField("loser_rank_points", "INT64"),
    bigquery.SchemaField("total_games", "INT64"),
    bigquery.SchemaField("winner_games", "INT64"),
    bigquery.SchemaField("loser_games", "INT64"),
    bigquery.SchemaField("sets_played", "INT64"),
]


def _split_dataset_table(table_id: str) -> Tuple[str, str]:
    parts = table_id.split(".")
    if len(parts) != 2:
        raise ValueError(f"Expected dataset.table format, got: {table_id}")
    return parts[0], parts[1]


def _table_ref(client: bigquery.Client, table_id: str) -> bigquery.TableReference:
    dataset_id, table_name = _split_dataset_table(table_id)
    return client.dataset(dataset_id).table(table_name)


def _fq_table(client: bigquery.Client, table_id: str) -> str:
    dataset_id, table_name = _split_dataset_table(table_id)
    return f"{client.project}.{dataset_id}.{table_name}"


def _ensure_dataset(client: bigquery.Client, dataset_id: str) -> None:
    dataset_ref = client.dataset(dataset_id)
    try:
        client.get_dataset(dataset_ref)
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = DEFAULT_LOCATION
        client.create_dataset(dataset)


def _ensure_table(
    client: bigquery.Client,
    table_id: str,
    schema: Sequence[bigquery.SchemaField],
) -> None:
    dataset_id, _ = _split_dataset_table(table_id)
    _ensure_dataset(client, dataset_id)
    table_ref = _table_ref(client, table_id)
    try:
        client.get_table(table_ref)
    except NotFound:
        table = bigquery.Table(table_ref, schema=list(schema))
        client.create_table(table)


def _to_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(float(text))
    except (TypeError, ValueError):
        return None


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def _parse_compact_date(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text or len(text) != 8 or not text.isdigit():
        return None
    try:
        return datetime.strptime(text, "%Y%m%d").date().isoformat()
    except ValueError:
        return None


def _normalize_name(value: Optional[str]) -> str:
    if not value:
        return ""
    return re.sub(r"[^a-z0-9]", "", value.strip().lower())


def _extract_score_metrics(score: Optional[str]) -> Tuple[Optional[int], Optional[int], Optional[int], Optional[int]]:
    if not score:
        return None, None, None, None
    pairs = _SCORE_RE.findall(score)
    if not pairs:
        return None, None, None, None
    winner_games = sum(int(a) for a, _ in pairs)
    loser_games = sum(int(b) for _, b in pairs)
    sets_played = len(pairs)
    total_games = winner_games + loser_games
    return total_games, winner_games, loser_games, sets_played


def _build_match_uid(row: Dict[str, Any], source_level: str, season: int) -> str:
    tourney_id = (row.get("tourney_id") or "").strip()
    match_num = (row.get("match_num") or "").strip()
    winner_id = (row.get("winner_id") or "").strip()
    loser_id = (row.get("loser_id") or "").strip()
    if tourney_id and match_num and winner_id and loser_id:
        return f"{season}:{source_level}:{tourney_id}:{match_num}:{winner_id}:{loser_id}"
    fallback = ":".join(
        [
            str(season),
            source_level,
            (row.get("tourney_date") or "").strip(),
            _normalize_name(row.get("winner_name")),
            _normalize_name(row.get("loser_name")),
            (row.get("round") or "").strip(),
        ]
    )
    return fallback


def _source_urls_for_year(
    year: int,
    include_challenger: bool,
    include_futures: bool,
) -> List[Tuple[str, str]]:
    sources: List[Tuple[str, str]] = [("main", f"{SACKMANN_RAW_BASE}/atp_matches_{year}.csv")]
    if include_challenger:
        sources.append(("qual_chall", f"{SACKMANN_RAW_BASE}/atp_matches_qual_chall_{year}.csv"))
    if include_futures:
        sources.append(("futures", f"{SACKMANN_RAW_BASE}/atp_matches_futures_{year}.csv"))
    return sources


def _download_csv_rows(url: str) -> List[Dict[str, str]]:
    response = requests.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
    if response.status_code == 404:
        return []
    response.raise_for_status()
    text_stream = io.StringIO(response.text)
    return list(csv.DictReader(text_stream))


def _build_raw_row(
    row: Dict[str, str],
    source_url: str,
    source_level: str,
    fallback_season: int,
) -> Dict[str, Any]:
    season = _to_int(str(row.get("tourney_date", ""))[:4]) or fallback_season
    total_games, winner_games, loser_games, sets_played = _extract_score_metrics(row.get("score"))
    return {
        "match_uid": _build_match_uid(row, source_level, season),
        "season": season,
        "source_level": source_level,
        "source_url": source_url,
        "source_repo": SACKMANN_SOURCE_REPO,
        "ingested_at": datetime.utcnow().isoformat(),
        "tourney_id": row.get("tourney_id"),
        "tourney_name": row.get("tourney_name"),
        "surface": row.get("surface"),
        "draw_size": _to_int(row.get("draw_size")),
        "tourney_level": row.get("tourney_level"),
        "tourney_date": _parse_compact_date(row.get("tourney_date")),
        "match_num": _to_int(row.get("match_num")),
        "best_of": _to_int(row.get("best_of")),
        "round": row.get("round"),
        "minutes": _to_int(row.get("minutes")),
        "winner_id": _to_int(row.get("winner_id")),
        "winner_name": row.get("winner_name"),
        "winner_ioc": row.get("winner_ioc"),
        "winner_age": _to_float(row.get("winner_age")),
        "loser_id": _to_int(row.get("loser_id")),
        "loser_name": row.get("loser_name"),
        "loser_ioc": row.get("loser_ioc"),
        "loser_age": _to_float(row.get("loser_age")),
        "score": row.get("score"),
        "w_ace": _to_int(row.get("w_ace")),
        "w_df": _to_int(row.get("w_df")),
        "l_ace": _to_int(row.get("l_ace")),
        "l_df": _to_int(row.get("l_df")),
        "winner_rank": _to_int(row.get("winner_rank")),
        "winner_rank_points": _to_int(row.get("winner_rank_points")),
        "loser_rank": _to_int(row.get("loser_rank")),
        "loser_rank_points": _to_int(row.get("loser_rank_points")),
        "total_games": total_games,
        "winner_games": winner_games,
        "loser_games": loser_games,
        "sets_played": sets_played,
    }


def _run_query(client: bigquery.Client, sql: str) -> None:
    client.query(sql).result()


def _replace_touched_raw_rows(
    client: bigquery.Client,
    rows: List[Dict[str, Any]],
    years: Sequence[int],
    levels: Sequence[str],
    truncate_raw: bool,
) -> None:
    _ensure_table(client, SACKMANN_RAW_TABLE, RAW_SCHEMA)
    raw_table = _fq_table(client, SACKMANN_RAW_TABLE)

    if truncate_raw:
        _run_query(client, f"TRUNCATE TABLE `{raw_table}`")
    elif years and levels:
        year_values = ", ".join(str(int(y)) for y in sorted(set(years)))
        level_values = ", ".join(f"'{str(level)}'" for level in sorted(set(levels)))
        _run_query(
            client,
            f"""
            DELETE FROM `{raw_table}`
            WHERE season IN ({year_values})
              AND source_level IN ({level_values})
            """,
        )

    if not rows:
        return

    table_ref = _table_ref(client, SACKMANN_RAW_TABLE)
    job = client.load_table_from_json(
        rows,
        table_ref,
        job_config=bigquery.LoadJobConfig(
            schema=RAW_SCHEMA,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        ),
    )
    job.result()


def rebuild_sackmann_features(client: Optional[bigquery.Client] = None) -> None:
    client = client or get_bq_client()
    raw_table = _fq_table(client, SACKMANN_RAW_TABLE)
    player_match_table = _fq_table(client, SACKMANN_PLAYER_MATCH_TABLE)
    player_surface_table = _fq_table(client, SACKMANN_PLAYER_SURFACE_FEATURES_TABLE)
    h2h_table = _fq_table(client, SACKMANN_H2H_FEATURES_TABLE)

    _run_query(
        client,
        f"""
        CREATE OR REPLACE TABLE `{player_match_table}` AS
        WITH base AS (
          SELECT
            match_uid,
            season,
            source_level,
            tourney_name,
            tourney_level,
            round,
            tourney_date AS match_date,
            LOWER(TRIM(surface)) AS surface_key,
            surface,
            best_of,
            minutes,
            total_games,
            sets_played,
            winner_name,
            loser_name,
            winner_id,
            loser_id,
            w_ace,
            l_ace,
            w_df,
            l_df,
            winner_games,
            loser_games,
            winner_rank,
            loser_rank,
            score
          FROM `{raw_table}`
          WHERE tourney_date IS NOT NULL
            AND winner_name IS NOT NULL
            AND loser_name IS NOT NULL
        )
        SELECT
          match_uid,
          season,
          source_level,
          match_date,
          surface_key,
          surface,
          tourney_name,
          tourney_level,
          round,
          score,
          TRUE AS is_win,
          winner_name AS player_name,
          LOWER(REGEXP_REPLACE(TRIM(winner_name), r'[^a-z0-9]', '')) AS player_name_norm,
          winner_id AS player_id,
          loser_name AS opponent_name,
          LOWER(REGEXP_REPLACE(TRIM(loser_name), r'[^a-z0-9]', '')) AS opponent_name_norm,
          loser_id AS opponent_id,
          w_ace AS aces,
          w_df AS double_faults,
          winner_games AS games_won,
          loser_games AS games_lost,
          total_games,
          sets_played,
          best_of,
          minutes,
          winner_rank AS player_rank,
          loser_rank AS opponent_rank
        FROM base
        UNION ALL
        SELECT
          match_uid,
          season,
          source_level,
          match_date,
          surface_key,
          surface,
          tourney_name,
          tourney_level,
          round,
          score,
          FALSE AS is_win,
          loser_name AS player_name,
          LOWER(REGEXP_REPLACE(TRIM(loser_name), r'[^a-z0-9]', '')) AS player_name_norm,
          loser_id AS player_id,
          winner_name AS opponent_name,
          LOWER(REGEXP_REPLACE(TRIM(winner_name), r'[^a-z0-9]', '')) AS opponent_name_norm,
          winner_id AS opponent_id,
          l_ace AS aces,
          l_df AS double_faults,
          loser_games AS games_won,
          winner_games AS games_lost,
          total_games,
          sets_played,
          best_of,
          minutes,
          loser_rank AS player_rank,
          winner_rank AS opponent_rank
        FROM base
        """,
    )

    _run_query(
        client,
        f"""
        CREATE OR REPLACE TABLE `{player_surface_table}` AS
        WITH ordered AS (
          SELECT
            *,
            ROW_NUMBER() OVER (
              PARTITION BY player_name_norm, surface_key
              ORDER BY match_date DESC, match_uid DESC
            ) AS rn
          FROM `{player_match_table}`
          WHERE player_name_norm IS NOT NULL
            AND player_name_norm != ''
            AND surface_key IS NOT NULL
            AND surface_key != ''
        )
        SELECT
          player_name_norm,
          ANY_VALUE(player_name) AS player_name,
          surface_key,
          COUNT(*) AS matches_played,
          SUM(CAST(is_win AS INT64)) AS wins,
          COUNT(*) - SUM(CAST(is_win AS INT64)) AS losses,
          SAFE_DIVIDE(SUM(CAST(is_win AS INT64)), COUNT(*)) AS win_rate,
          AVG(CAST(aces AS FLOAT64)) AS aces_per_match,
          AVG(CAST(double_faults AS FLOAT64)) AS double_faults_per_match,
          AVG(CAST(total_games AS FLOAT64)) AS avg_games_per_match,
          AVG(CAST(sets_played AS FLOAT64)) AS avg_sets_per_match,
          AVG(IF(rn <= 5, CAST(aces AS FLOAT64), NULL)) AS recent_aces_l5_avg,
          AVG(IF(rn <= 5, CAST(double_faults AS FLOAT64), NULL)) AS recent_double_faults_l5_avg,
          AVG(IF(rn <= 5, CAST(total_games AS FLOAT64), NULL)) AS recent_avg_games_l5,
          AVG(IF(rn <= 5, CAST(sets_played AS FLOAT64), NULL)) AS recent_avg_sets_l5,
          TO_JSON_STRING(
            ARRAY_AGG(STRUCT(match_date, aces) ORDER BY match_date DESC, match_uid DESC LIMIT 10)
          ) AS recent_aces_by_match,
          TO_JSON_STRING(
            ARRAY_AGG(STRUCT(match_date, double_faults) ORDER BY match_date DESC, match_uid DESC LIMIT 10)
          ) AS recent_double_faults_by_match,
          ARRAY_TO_STRING(
            ARRAY_AGG(IF(is_win, 'W', 'L') ORDER BY match_date DESC, match_uid DESC LIMIT 10),
            ''
          ) AS recent_form_last10,
          CURRENT_TIMESTAMP() AS updated_at
        FROM ordered
        GROUP BY player_name_norm, surface_key
        """,
    )

    _run_query(
        client,
        f"""
        CREATE OR REPLACE TABLE `{h2h_table}` AS
        WITH directional AS (
          SELECT
            player_name_norm,
            player_name,
            opponent_name_norm,
            opponent_name,
            surface_key,
            match_uid,
            match_date,
            tourney_name,
            round,
            score,
            is_win,
            aces,
            double_faults,
            total_games,
            sets_played
          FROM `{player_match_table}`
          WHERE player_name_norm IS NOT NULL
            AND player_name_norm != ''
            AND opponent_name_norm IS NOT NULL
            AND opponent_name_norm != ''
            AND surface_key IS NOT NULL
            AND surface_key != ''
        ),
        all_surface AS (
          SELECT
            player_name_norm,
            player_name,
            opponent_name_norm,
            opponent_name,
            'all' AS surface_key,
            match_uid,
            match_date,
            tourney_name,
            round,
            score,
            is_win,
            aces,
            double_faults,
            total_games,
            sets_played
          FROM directional
        ),
        unioned AS (
          SELECT * FROM directional
          UNION ALL
          SELECT * FROM all_surface
        )
        SELECT
          player_name_norm,
          ANY_VALUE(player_name) AS player_name,
          opponent_name_norm,
          ANY_VALUE(opponent_name) AS opponent_name,
          surface_key,
          COUNT(*) AS matches_played,
          SUM(CAST(is_win AS INT64)) AS wins,
          COUNT(*) - SUM(CAST(is_win AS INT64)) AS losses,
          SAFE_DIVIDE(SUM(CAST(is_win AS INT64)), COUNT(*)) AS win_rate,
          AVG(CAST(aces AS FLOAT64)) AS aces_per_match,
          AVG(CAST(double_faults AS FLOAT64)) AS double_faults_per_match,
          AVG(CAST(total_games AS FLOAT64)) AS avg_games_per_match,
          AVG(CAST(sets_played AS FLOAT64)) AS avg_sets_per_match,
          TO_JSON_STRING(
            ARRAY_AGG(
              STRUCT(match_date, is_win, tourney_name, round, score, aces, double_faults, total_games, sets_played)
              ORDER BY match_date DESC, match_uid DESC
              LIMIT 10
            )
          ) AS recent_h2h_matches,
          CURRENT_TIMESTAMP() AS updated_at
        FROM unioned
        GROUP BY player_name_norm, opponent_name_norm, surface_key
        """,
    )


def ingest_sackmann_years(
    *,
    years: Sequence[int],
    include_challenger: bool = True,
    include_futures: bool = False,
    truncate_raw: bool = False,
    rebuild_features: bool = True,
) -> Dict[str, Any]:
    client = get_bq_client()
    normalized_years = sorted({int(y) for y in years if y})
    if not normalized_years:
        raise ValueError("No years were supplied for Sackmann ingest.")

    collected_rows: List[Dict[str, Any]] = []
    touched_levels: List[str] = []
    files_attempted = 0
    files_loaded = 0

    for year in normalized_years:
        for source_level, source_url in _source_urls_for_year(
            year=year,
            include_challenger=include_challenger,
            include_futures=include_futures,
        ):
            files_attempted += 1
            csv_rows = _download_csv_rows(source_url)
            if not csv_rows:
                continue
            files_loaded += 1
            touched_levels.append(source_level)
            for csv_row in csv_rows:
                collected_rows.append(_build_raw_row(csv_row, source_url, source_level, year))

    dedupe: Dict[str, Dict[str, Any]] = {}
    for row in collected_rows:
        dedupe_key = f"{row['source_level']}::{row['match_uid']}"
        dedupe[dedupe_key] = row
    deduped_rows = list(dedupe.values())

    if truncate_raw and not deduped_rows:
        raise RuntimeError(
            "Refusing to truncate Sackmann raw table because no rows were downloaded."
        )

    _replace_touched_raw_rows(
        client=client,
        rows=deduped_rows,
        years=normalized_years,
        levels=touched_levels,
        truncate_raw=truncate_raw,
    )

    if not deduped_rows:
        return {
            "years": normalized_years,
            "rows_loaded": 0,
            "files_attempted": files_attempted,
            "files_loaded": files_loaded,
            "include_challenger": include_challenger,
            "include_futures": include_futures,
            "truncate_raw": truncate_raw,
            "rebuild_features": False,
            "raw_table": SACKMANN_RAW_TABLE,
            "player_match_table": SACKMANN_PLAYER_MATCH_TABLE,
            "player_surface_features_table": SACKMANN_PLAYER_SURFACE_FEATURES_TABLE,
            "h2h_features_table": SACKMANN_H2H_FEATURES_TABLE,
            "note": "No Sackmann source rows downloaded for requested years; feature rebuild skipped.",
        }

    if rebuild_features:
        rebuild_sackmann_features(client=client)

    return {
        "years": normalized_years,
        "rows_loaded": len(deduped_rows),
        "files_attempted": files_attempted,
        "files_loaded": files_loaded,
        "include_challenger": include_challenger,
        "include_futures": include_futures,
        "truncate_raw": truncate_raw,
        "rebuild_features": rebuild_features,
        "raw_table": SACKMANN_RAW_TABLE,
        "player_match_table": SACKMANN_PLAYER_MATCH_TABLE,
        "player_surface_features_table": SACKMANN_PLAYER_SURFACE_FEATURES_TABLE,
        "h2h_features_table": SACKMANN_H2H_FEATURES_TABLE,
    }


def ingest_sackmann_backfill(
    *,
    start_year: int,
    end_year: int,
    include_challenger: bool = True,
    include_futures: bool = False,
    truncate_raw: bool = True,
    rebuild_features: bool = True,
) -> Dict[str, Any]:
    if end_year < start_year:
        raise ValueError("end_year must be >= start_year")
    years = list(range(start_year, end_year + 1))
    return ingest_sackmann_years(
        years=years,
        include_challenger=include_challenger,
        include_futures=include_futures,
        truncate_raw=truncate_raw,
        rebuild_features=rebuild_features,
    )


def ingest_sackmann_daily(
    *,
    include_challenger: bool = True,
    include_futures: bool = False,
    years_back: int = 2,
    rebuild_features: bool = True,
) -> Dict[str, Any]:
    now_year = datetime.utcnow().year
    years = [now_year - i for i in range(max(0, years_back) + 1)]
    return ingest_sackmann_years(
        years=years,
        include_challenger=include_challenger,
        include_futures=include_futures,
        truncate_raw=False,
        rebuild_features=rebuild_features,
    )


def _parse_csv_years(value: Optional[str]) -> List[int]:
    if not value:
        return []
    return [int(v.strip()) for v in value.split(",") if v.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest Jeff Sackmann ATP data into BigQuery.")
    parser.add_argument("--mode", choices=["backfill", "daily", "rebuild-features"], default="daily")
    parser.add_argument("--start-year", type=int, default=1968)
    parser.add_argument("--end-year", type=int, default=datetime.utcnow().year)
    parser.add_argument("--years", type=str, default="")
    parser.add_argument("--include-challenger", dest="include_challenger", action="store_true")
    parser.add_argument("--no-include-challenger", dest="include_challenger", action="store_false")
    parser.add_argument("--include-futures", dest="include_futures", action="store_true")
    parser.add_argument("--no-include-futures", dest="include_futures", action="store_false")
    parser.add_argument("--no-rebuild-features", action="store_true")
    parser.add_argument("--truncate-raw", action="store_true")
    parser.add_argument("--years-back", type=int, default=2)
    parser.set_defaults(include_challenger=True, include_futures=False)
    args = parser.parse_args()

    rebuild_features = not args.no_rebuild_features

    if args.mode == "rebuild-features":
        rebuild_sackmann_features()
        print(json.dumps({"ok": True, "mode": "rebuild-features"}))
        return

    if args.mode == "backfill":
        explicit_years = _parse_csv_years(args.years)
        if explicit_years:
            result = ingest_sackmann_years(
                years=explicit_years,
                include_challenger=args.include_challenger,
                include_futures=args.include_futures,
                truncate_raw=args.truncate_raw,
                rebuild_features=rebuild_features,
            )
        else:
            result = ingest_sackmann_backfill(
                start_year=args.start_year,
                end_year=args.end_year,
                include_challenger=args.include_challenger,
                include_futures=args.include_futures,
                truncate_raw=args.truncate_raw,
                rebuild_features=rebuild_features,
            )
        print(json.dumps(result))
        return

    explicit_years = _parse_csv_years(args.years)
    if explicit_years:
        result = ingest_sackmann_years(
            years=explicit_years,
            include_challenger=args.include_challenger,
            include_futures=args.include_futures,
            truncate_raw=False,
            rebuild_features=rebuild_features,
        )
    else:
        result = ingest_sackmann_daily(
            include_challenger=args.include_challenger,
            include_futures=args.include_futures,
            years_back=args.years_back,
            rebuild_features=rebuild_features,
        )
    print(json.dumps(result))


if __name__ == "__main__":
    main()


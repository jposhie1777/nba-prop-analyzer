from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, List, Optional, Sequence, Tuple

from fastapi import APIRouter, Query
from google.api_core.exceptions import BadRequest, NotFound
from google.cloud import bigquery

from bq import get_bq_client

router = APIRouter(prefix="/atp", tags=["ATP Matchups"])


ATP_TABLES: Dict[str, str] = {
    "upcoming": os.getenv("ODDSPEDIA_ATP_UPCOMING_MATCHES_TABLE", "oddspedia.atp_upcoming_matches"),
    "match_weather": os.getenv("ODDSPEDIA_ATP_MATCH_WEATHER_TABLE", "oddspedia.atp_match_weather"),
    "match_keys": os.getenv("ODDSPEDIA_ATP_MATCH_KEYS_TABLE", "oddspedia.atp_match_keys"),
    "betting_stats": os.getenv("ODDSPEDIA_ATP_BETTING_STATS_TABLE", "oddspedia.atp_betting_stats"),
    "h2h_summary": os.getenv("ODDSPEDIA_ATP_H2H_SUMMARY_TABLE", "oddspedia.atp_h2h_summary"),
    "h2h_matches": os.getenv("ODDSPEDIA_ATP_H2H_MATCHES_TABLE", "oddspedia.atp_h2h_matches"),
    "last_matches": os.getenv("ODDSPEDIA_ATP_LAST_MATCHES_TABLE", "oddspedia.atp_last_matches"),
    "odds": os.getenv("ODDSPEDIA_ATP_ODDS_TABLE", "oddspedia.atp_odds"),
}
PLAYER_LOOKUP_TABLE = os.getenv("ATP_PLAYER_LOOKUP_TABLE", "atp_data.player_lookup")
SACKMANN_PLAYER_SURFACE_FEATURES_TABLE = os.getenv(
    "ATP_SACKMANN_PLAYER_SURFACE_FEATURES_TABLE",
    "atp_data.sackmann_player_surface_features",
)
SACKMANN_H2H_FEATURES_TABLE = os.getenv(
    "ATP_SACKMANN_H2H_FEATURES_TABLE",
    "atp_data.sackmann_h2h_features",
)
SACKMANN_SOURCE_REPO = os.getenv("SACKMANN_SOURCE_REPO", "JeffSackmann/tennis_atp")


def _split_table_ref(table_ref: str) -> tuple[str | None, str, str]:
    parts = table_ref.split(".")
    if len(parts) == 2:
        return None, parts[0], parts[1]
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    raise ValueError(f"Unsupported BigQuery table reference: {table_ref}")


def _query(sql: str, params: Sequence[bigquery.ScalarQueryParameter]) -> List[Dict[str, Any]]:
    client = get_bq_client()
    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(query_parameters=list(params)),
    )
    return [dict(row) for row in job.result()]


def _safe_query(sql: str, params: Sequence[bigquery.ScalarQueryParameter]) -> List[Dict[str, Any]]:
    try:
        return _query(sql, params)
    except (NotFound, BadRequest):
        return []


def _table_columns(table_ref: str) -> set[str]:
    try:
        project, dataset, table = _split_table_ref(table_ref)
    except ValueError:
        return set()

    client = get_bq_client()
    project = project or client.project
    sql = f"""
    SELECT column_name
    FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = @table
    """
    rows = _safe_query(sql, [bigquery.ScalarQueryParameter("table", "STRING", table)])
    return {row.get("column_name") for row in rows if row.get("column_name")}


def _order_sql_for_columns(columns: set[str]) -> str:
    order_parts: List[str] = []
    if "ingested_at" in columns:
        order_parts.append("SAFE_CAST(ingested_at AS TIMESTAMP) DESC")
    if "date_utc" in columns:
        order_parts.append("SAFE_CAST(date_utc AS TIMESTAMP) DESC")
    if "start_time_utc" in columns:
        order_parts.append("SAFE_CAST(start_time_utc AS TIMESTAMP) DESC")
    if "last_starttime" in columns:
        order_parts.append("SAFE_CAST(last_starttime AS TIMESTAMP) DESC")
    if "h2h_starttime" in columns:
        order_parts.append("SAFE_CAST(h2h_starttime AS TIMESTAMP) DESC")
    if "rank" in columns:
        order_parts.append("SAFE_CAST(rank AS INT64) ASC")
    return ", ".join(order_parts) if order_parts else "match_id DESC"


def _normalize_name_key(value: Optional[str]) -> str:
    if not value:
        return ""
    return "".join(ch for ch in value.lower().strip() if ch.isalnum() or ch == " ")


def _normalize_name_norm(value: Optional[str]) -> str:
    if not value:
        return ""
    return "".join(ch for ch in value.lower().strip() if ch.isalnum())


def _normalize_surface_key(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    cleaned = str(value).strip().lower()
    if not cleaned:
        return None
    if "hard" in cleaned:
        return "hard"
    if "clay" in cleaned:
        return "clay"
    if "grass" in cleaned:
        return "grass"
    token = re.sub(r"[^a-z]", "", cleaned.split(" ")[0])
    return token or None


def _safe_json_load(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return None
    return None


def _fetch_player_headshots(player_names: Sequence[str]) -> Dict[str, str]:
    cleaned = [name.strip() for name in player_names if name and name.strip()]
    if not cleaned:
        return {}

    lower_names = [name.lower() for name in cleaned]
    normalized_names = [_normalize_name_key(name) for name in cleaned if _normalize_name_key(name)]
    if not normalized_names:
        normalized_names = [name.lower() for name in cleaned]

    sql = f"""
    WITH targets AS (
      SELECT
        name,
        LOWER(TRIM(name)) AS exact_key,
        LOWER(REGEXP_REPLACE(TRIM(name), r'[^a-z0-9 ]', '')) AS normalized_key
      FROM UNNEST(@names) AS name
    ),
    candidates AS (
      SELECT
        player_name,
        player_image_url,
        last_verified,
        LOWER(TRIM(player_name)) AS exact_key,
        LOWER(REGEXP_REPLACE(TRIM(player_name), r'[^a-z0-9 ]', '')) AS normalized_key
      FROM `{PLAYER_LOOKUP_TABLE}`
      WHERE player_image_url IS NOT NULL
        AND (
          LOWER(TRIM(player_name)) IN UNNEST(@lower_names)
          OR LOWER(REGEXP_REPLACE(TRIM(player_name), r'[^a-z0-9 ]', '')) IN UNNEST(@normalized_names)
        )
    )
    SELECT
      targets.name AS target_name,
      candidates.player_image_url AS player_image_url
    FROM targets
    LEFT JOIN candidates
      ON targets.exact_key = candidates.exact_key
      OR (
        targets.normalized_key != ''
        AND targets.normalized_key = candidates.normalized_key
      )
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY targets.name
      ORDER BY
        IF(targets.exact_key = candidates.exact_key, 0, 1),
        candidates.last_verified DESC NULLS LAST
    ) = 1
    """
    rows = _safe_query(
        sql,
        [
            bigquery.ArrayQueryParameter("names", "STRING", cleaned),
            bigquery.ArrayQueryParameter("lower_names", "STRING", lower_names),
            bigquery.ArrayQueryParameter("normalized_names", "STRING", normalized_names),
        ],
    )
    out: Dict[str, str] = {}
    for row in rows:
        target_name = row.get("target_name")
        url = row.get("player_image_url")
        if target_name and url:
            out[str(target_name)] = str(url)
    return out


def _fetch_upcoming_rows(limit: int, lookahead_days: int) -> List[Dict[str, Any]]:
    upcoming_columns = _table_columns(ATP_TABLES["upcoming"])
    required = {"match_id", "home_team", "away_team", "start_time_utc"}
    if not required.issubset(upcoming_columns):
        return []

    weather_columns = _table_columns(ATP_TABLES["match_weather"])
    has_weather = "match_id" in weather_columns

    weather_join_sql = ""
    weather_select_sql = """
      CAST(NULL AS STRING) AS round_name,
      CAST(NULL AS STRING) AS home_rank,
      CAST(NULL AS STRING) AS away_rank,
      CAST(NULL AS TIMESTAMP) AS match_date_utc
    """
    if has_weather:
        weather_join_sql = f"""
        LEFT JOIN (
          SELECT
            CAST(match_id AS INT64) AS match_id,
            round_name,
            ht_rank,
            at_rank,
            SAFE_CAST(date_utc AS TIMESTAMP) AS date_utc,
            ROW_NUMBER() OVER (
              PARTITION BY CAST(match_id AS INT64)
              ORDER BY SAFE_CAST(ingested_at AS TIMESTAMP) DESC NULLS LAST, SAFE_CAST(date_utc AS TIMESTAMP) DESC NULLS LAST
            ) AS rn
          FROM `{ATP_TABLES["match_weather"]}`
        ) weather
          ON weather.match_id = upcoming.match_id
         AND weather.rn = 1
        """
        weather_select_sql = """
          weather.round_name AS round_name,
          weather.ht_rank AS home_rank,
          weather.at_rank AS away_rank,
          weather.date_utc AS match_date_utc
        """

    sql = f"""
    WITH upcoming AS (
      SELECT
        CAST(match_id AS INT64) AS match_id,
        home_team,
        away_team,
        matchup,
        tournament_name,
        SAFE_CAST(start_time_utc AS TIMESTAMP) AS start_time_utc,
        SAFE_CAST(ingested_at AS TIMESTAMP) AS ingested_at,
        ROW_NUMBER() OVER (
          PARTITION BY CAST(match_id AS INT64)
          ORDER BY SAFE_CAST(ingested_at AS TIMESTAMP) DESC NULLS LAST, SAFE_CAST(start_time_utc AS TIMESTAMP) ASC
        ) AS rn
      FROM `{ATP_TABLES["upcoming"]}`
      WHERE CAST(match_id AS INT64) IS NOT NULL
    )
    SELECT
      upcoming.match_id,
      upcoming.home_team,
      upcoming.away_team,
      COALESCE(NULLIF(upcoming.matchup, ''), CONCAT(upcoming.home_team, ' vs ', upcoming.away_team)) AS matchup,
      upcoming.tournament_name,
      upcoming.start_time_utc,
      {weather_select_sql}
    FROM upcoming
    {weather_join_sql}
    WHERE upcoming.rn = 1
      AND upcoming.start_time_utc IS NOT NULL
      AND DATE(upcoming.start_time_utc) BETWEEN CURRENT_DATE() AND DATE_ADD(CURRENT_DATE(), INTERVAL @lookahead_days DAY)
    ORDER BY upcoming.start_time_utc ASC, matchup ASC
    LIMIT @limit
    """
    return _safe_query(
        sql,
        [
            bigquery.ScalarQueryParameter("lookahead_days", "INT64", lookahead_days),
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
        ],
    )


def _normalize_atp_outcome(value: Any) -> Optional[str]:
    if value is None:
        return None
    normalized = str(value).strip().lower()
    if not normalized:
        return None
    if normalized in {"home", "o1", "1"}:
        return "home"
    if normalized in {"away", "o2", "2"}:
        return "away"
    return None


def _fetch_odds_summary_for_matches(match_ids: Sequence[int]) -> Dict[int, Dict[str, Any]]:
    if not match_ids:
        return {}

    odds_columns = _table_columns(ATP_TABLES["odds"])
    if "match_id" not in odds_columns or not ({"market"} & odds_columns or {"market_group_name"} & odds_columns):
        return {}

    sql = f"""
    WITH latest AS (
      SELECT
        CAST(match_id AS INT64) AS match_id,
        LOWER(
          COALESCE(
            NULLIF(TRIM(CAST(outcome_side AS STRING)), ''),
            NULLIF(TRIM(CAST(outcome_key AS STRING)), ''),
            NULLIF(TRIM(CAST(outcome_name AS STRING)), '')
          )
        ) AS raw_outcome,
        NULLIF(TRIM(CAST(bookie AS STRING)), '') AS bookie,
        SAFE_CAST(odds_decimal AS FLOAT64) AS odds_decimal,
        SAFE_CAST(odds_american AS INT64) AS odds_american,
        SAFE_CAST(ingested_at AS TIMESTAMP) AS ingested_at,
        ROW_NUMBER() OVER (
          PARTITION BY
            CAST(match_id AS INT64),
            LOWER(
              COALESCE(
                NULLIF(TRIM(CAST(outcome_side AS STRING)), ''),
                NULLIF(TRIM(CAST(outcome_key AS STRING)), ''),
                NULLIF(TRIM(CAST(outcome_name AS STRING)), '')
              )
            ),
            COALESCE(NULLIF(TRIM(CAST(bookie AS STRING)), ''), '')
          ORDER BY SAFE_CAST(ingested_at AS TIMESTAMP) DESC NULLS LAST
        ) AS rn
      FROM `{ATP_TABLES["odds"]}`
      WHERE CAST(match_id AS INT64) IN UNNEST(@match_ids)
        AND LOWER(
          COALESCE(
            NULLIF(TRIM(CAST(market_group_name AS STRING)), ''),
            NULLIF(TRIM(CAST(market AS STRING)), '')
          )
        ) IN ('moneyline', 'h2h', 'winner', 'match_winner')
    ),
    deduped AS (
      SELECT *
      FROM latest
      WHERE rn = 1
    ),
    ranked AS (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY match_id, raw_outcome
          ORDER BY odds_decimal DESC NULLS LAST, ingested_at DESC NULLS LAST
        ) AS best_rank
      FROM deduped
      WHERE raw_outcome IS NOT NULL
    )
    SELECT
      match_id,
      raw_outcome,
      bookie,
      odds_decimal,
      odds_american,
      ingested_at
    FROM ranked
    WHERE best_rank = 1
    """
    rows = _safe_query(
        sql,
        [bigquery.ArrayQueryParameter("match_ids", "INT64", [int(match_id) for match_id in match_ids])],
    )

    out: Dict[int, Dict[str, Any]] = {}
    for row in rows:
        match_id = row.get("match_id")
        if match_id is None:
            continue
        outcome = _normalize_atp_outcome(row.get("raw_outcome"))
        if outcome is None:
            continue
        match_key = int(match_id)
        if match_key not in out:
            out[match_key] = {"home": None, "away": None, "updated_at": None}
        out[match_key][outcome] = {
            "bookie": row.get("bookie"),
            "odds_decimal": row.get("odds_decimal"),
            "odds_american": row.get("odds_american"),
        }
        ingested_at = row.get("ingested_at")
        current_updated_at = out[match_key].get("updated_at")
        if ingested_at is not None and (current_updated_at is None or ingested_at > current_updated_at):
            out[match_key]["updated_at"] = ingested_at
    return out


def _fetch_match_odds_board(
    match_id: int,
    *,
    max_rows: int = 500,
    top_books_per_outcome: int = 2,
) -> Tuple[List[Dict[str, Any]], Optional[Any]]:
    odds_columns = _table_columns(ATP_TABLES["odds"])
    if "match_id" not in odds_columns:
        return [], None

    sql = f"""
    WITH latest AS (
      SELECT
        CAST(match_id AS INT64) AS match_id,
        COALESCE(
          NULLIF(TRIM(CAST(market_group_name AS STRING)), ''),
          NULLIF(TRIM(CAST(market AS STRING)), ''),
          'Other'
        ) AS market_group,
        COALESCE(
          NULLIF(TRIM(CAST(market AS STRING)), ''),
          NULLIF(TRIM(CAST(market_group_name AS STRING)), ''),
          'other'
        ) AS market,
        SAFE_CAST(period_id AS INT64) AS period_id,
        NULLIF(TRIM(CAST(period_name AS STRING)), '') AS period_name,
        NULLIF(TRIM(CAST(line_value AS STRING)), '') AS line_value,
        COALESCE(
          NULLIF(TRIM(CAST(outcome_name AS STRING)), ''),
          NULLIF(TRIM(CAST(outcome_key AS STRING)), ''),
          NULLIF(TRIM(CAST(outcome_side AS STRING)), '')
        ) AS outcome_name,
        NULLIF(TRIM(CAST(outcome_side AS STRING)), '') AS outcome_side,
        SAFE_CAST(outcome_order AS INT64) AS outcome_order,
        NULLIF(TRIM(CAST(bookie AS STRING)), '') AS bookie,
        SAFE_CAST(odds_decimal AS FLOAT64) AS odds_decimal,
        SAFE_CAST(odds_american AS INT64) AS odds_american,
        SAFE_CAST(ingested_at AS TIMESTAMP) AS ingested_at,
        ROW_NUMBER() OVER (
          PARTITION BY
            CAST(match_id AS INT64),
            COALESCE(NULLIF(TRIM(CAST(market_group_name AS STRING)), ''), NULLIF(TRIM(CAST(market AS STRING)), ''), 'Other'),
            COALESCE(NULLIF(TRIM(CAST(market AS STRING)), ''), NULLIF(TRIM(CAST(market_group_name AS STRING)), ''), 'other'),
            COALESCE(CAST(SAFE_CAST(period_id AS INT64) AS STRING), ''),
            COALESCE(NULLIF(TRIM(CAST(period_name AS STRING)), ''), ''),
            COALESCE(NULLIF(TRIM(CAST(line_value AS STRING)), ''), ''),
            COALESCE(NULLIF(TRIM(CAST(outcome_name AS STRING)), ''), NULLIF(TRIM(CAST(outcome_key AS STRING)), ''), NULLIF(TRIM(CAST(outcome_side AS STRING)), ''), ''),
            COALESCE(NULLIF(TRIM(CAST(bookie AS STRING)), ''), '')
          ORDER BY SAFE_CAST(ingested_at AS TIMESTAMP) DESC NULLS LAST
        ) AS rn
      FROM `{ATP_TABLES["odds"]}`
      WHERE CAST(match_id AS INT64) = @match_id
    ),
    deduped AS (
      SELECT *
      FROM latest
      WHERE rn = 1
        AND outcome_name IS NOT NULL
    ),
    ranked AS (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY
            match_id,
            market_group,
            market,
            COALESCE(CAST(period_id AS STRING), ''),
            COALESCE(period_name, ''),
            COALESCE(line_value, ''),
            LOWER(outcome_name)
          ORDER BY odds_decimal DESC NULLS LAST, ingested_at DESC NULLS LAST
        ) AS best_rank
      FROM deduped
    )
    SELECT
      market_group,
      market,
      period_id,
      period_name,
      line_value,
      outcome_name,
      outcome_side,
      outcome_order,
      bookie,
      odds_decimal,
      odds_american,
      ingested_at
    FROM ranked
    WHERE best_rank <= @top_books_per_outcome
    ORDER BY
      market_group ASC,
      market ASC,
      period_id ASC NULLS FIRST,
      period_name ASC NULLS FIRST,
      line_value ASC NULLS FIRST,
      outcome_order ASC NULLS LAST,
      outcome_name ASC,
      best_rank ASC
    LIMIT @max_rows
    """
    rows = _safe_query(
        sql,
        [
            bigquery.ScalarQueryParameter("match_id", "INT64", match_id),
            bigquery.ScalarQueryParameter("top_books_per_outcome", "INT64", top_books_per_outcome),
            bigquery.ScalarQueryParameter("max_rows", "INT64", max_rows),
        ],
    )

    latest_ingested_at = None
    for row in rows:
        ingested_at = row.get("ingested_at")
        if ingested_at is not None and (latest_ingested_at is None or ingested_at > latest_ingested_at):
            latest_ingested_at = ingested_at
    return rows, latest_ingested_at


def _fetch_single_match_row(table_ref: str, match_id: int, fields: Sequence[str]) -> Optional[Dict[str, Any]]:
    columns = _table_columns(table_ref)
    if "match_id" not in columns:
        return None
    selected_fields = [field for field in fields if field in columns]
    if not selected_fields:
        return None
    order_sql = _order_sql_for_columns(columns)
    sql = f"""
    SELECT {", ".join(selected_fields)}
    FROM `{table_ref}`
    WHERE CAST(match_id AS INT64) = @match_id
    ORDER BY {order_sql}
    LIMIT 1
    """
    rows = _safe_query(sql, [bigquery.ScalarQueryParameter("match_id", "INT64", match_id)])
    return rows[0] if rows else None


def _fetch_rows_for_match(
    table_ref: str,
    match_id: int,
    fields: Sequence[str],
    *,
    limit: int,
    order_sql_override: Optional[str] = None,
) -> List[Dict[str, Any]]:
    columns = _table_columns(table_ref)
    if "match_id" not in columns:
        return []
    selected_fields = [field for field in fields if field in columns]
    if not selected_fields:
        return []

    order_sql = order_sql_override or _order_sql_for_columns(columns)
    sql = f"""
    SELECT {", ".join(selected_fields)}
    FROM `{table_ref}`
    WHERE CAST(match_id AS INT64) = @match_id
    ORDER BY {order_sql}
    LIMIT @limit
    """
    return _safe_query(
        sql,
        [
            bigquery.ScalarQueryParameter("match_id", "INT64", match_id),
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
        ],
    )


def _fetch_upcoming_match_row(match_id: int) -> Optional[Dict[str, Any]]:
    return _fetch_single_match_row(
        ATP_TABLES["upcoming"],
        match_id,
        [
            "match_id",
            "home_team",
            "away_team",
            "matchup",
            "start_time_utc",
            "tournament_name",
            "ingested_at",
            "scraped_date",
        ],
    )


def _fetch_match_weather_row(match_id: int) -> Optional[Dict[str, Any]]:
    return _fetch_single_match_row(
        ATP_TABLES["match_weather"],
        match_id,
        [
            "match_id",
            "match_key",
            "home_team",
            "away_team",
            "date_utc",
            "round_name",
            "weather_icon",
            "weather_temp_c",
            "surface",
            "prize_money",
            "prize_currency",
            "ht_rank",
            "at_rank",
            "ingested_at",
            "scraped_date",
        ],
    )


def _fetch_head_to_head_summary_row(match_id: int) -> Optional[Dict[str, Any]]:
    return _fetch_single_match_row(
        ATP_TABLES["h2h_summary"],
        match_id,
        [
            "match_id",
            "match_key",
            "home_team",
            "away_team",
            "date_utc",
            "round_name",
            "ht_wins",
            "at_wins",
            "draws",
            "played_matches",
            "period_years",
            "ingested_at",
            "scraped_date",
        ],
    )


def _fetch_match_keys_rows(match_id: int) -> List[Dict[str, Any]]:
    return _fetch_rows_for_match(
        ATP_TABLES["match_keys"],
        match_id,
        ["match_id", "match_key", "rank", "statement", "teams_json", "round_name", "ingested_at", "scraped_date"],
        limit=200,
        order_sql_override="SAFE_CAST(rank AS INT64) ASC, SAFE_CAST(ingested_at AS TIMESTAMP) DESC NULLS LAST",
    )


def _fetch_betting_stats_rows(match_id: int) -> List[Dict[str, Any]]:
    return _fetch_rows_for_match(
        ATP_TABLES["betting_stats"],
        match_id,
        [
            "match_id",
            "match_key",
            "home_team",
            "away_team",
            "date_utc",
            "round_name",
            "category",
            "sub_tab",
            "label",
            "value",
            "home",
            "away",
            "total_matches_home",
            "total_matches_away",
            "ingested_at",
            "scraped_date",
        ],
        limit=500,
        order_sql_override=(
            "category ASC, sub_tab ASC NULLS FIRST, label ASC, "
            "SAFE_CAST(ingested_at AS TIMESTAMP) DESC NULLS LAST"
        ),
    )


def _fetch_head_to_head_match_rows(match_id: int) -> List[Dict[str, Any]]:
    return _fetch_rows_for_match(
        ATP_TABLES["h2h_matches"],
        match_id,
        [
            "match_id",
            "match_key",
            "home_team",
            "away_team",
            "date_utc",
            "round_name",
            "h2h_match_id",
            "h2h_starttime",
            "h2h_ht",
            "h2h_ht_id",
            "h2h_at",
            "h2h_at_id",
            "h2h_hscore",
            "h2h_ascore",
            "h2h_winner",
            "h2h_league_name",
            "h2h_league_slug",
            "h2h_is_archived",
            "h2h_periods_json",
            "ingested_at",
            "scraped_date",
        ],
        limit=100,
        order_sql_override="SAFE_CAST(h2h_starttime AS TIMESTAMP) DESC NULLS LAST, SAFE_CAST(ingested_at AS TIMESTAMP) DESC NULLS LAST",
    )


def _fetch_recent_match_rows(match_id: int) -> List[Dict[str, Any]]:
    return _fetch_rows_for_match(
        ATP_TABLES["last_matches"],
        match_id,
        [
            "match_id",
            "match_key",
            "home_team",
            "away_team",
            "date_utc",
            "round_name",
            "side",
            "player_id",
            "last_match_id",
            "last_starttime",
            "last_ht",
            "last_at",
            "last_hscore",
            "last_ascore",
            "last_outcome",
            "last_home",
            "last_status_json",
            "last_league_id",
            "ingested_at",
            "scraped_date",
        ],
        limit=120,
        order_sql_override=(
            "side ASC, SAFE_CAST(last_starttime AS TIMESTAMP) DESC NULLS LAST, "
            "SAFE_CAST(ingested_at AS TIMESTAMP) DESC NULLS LAST"
        ),
    )


def _fetch_sackmann_player_surface_rows(
    player_norms: Sequence[str],
    *,
    surface_key: Optional[str],
) -> List[Dict[str, Any]]:
    norms = sorted({norm for norm in player_norms if norm})
    if not norms:
        return []

    columns = _table_columns(SACKMANN_PLAYER_SURFACE_FEATURES_TABLE)
    required = {"player_name_norm", "surface_key"}
    if not required.issubset(columns):
        return []

    selected_fields = [
        field
        for field in [
            "player_name_norm",
            "player_name",
            "surface_key",
            "matches_played",
            "wins",
            "losses",
            "win_rate",
            "aces_per_match",
            "double_faults_per_match",
            "avg_games_per_match",
            "avg_sets_per_match",
            "recent_aces_l5_avg",
            "recent_double_faults_l5_avg",
            "recent_avg_games_l5",
            "recent_avg_sets_l5",
            "recent_aces_by_match",
            "recent_double_faults_by_match",
            "recent_form_last10",
            "updated_at",
        ]
        if field in columns
    ]
    if not selected_fields:
        return []

    where_surface = "TRUE"
    params: List[bigquery.ScalarQueryParameter | bigquery.ArrayQueryParameter] = [
        bigquery.ArrayQueryParameter("player_norms", "STRING", norms)
    ]
    if surface_key and "surface_key" in columns:
        where_surface = "surface_key = @surface_key"
        params.append(bigquery.ScalarQueryParameter("surface_key", "STRING", surface_key))

    sql = f"""
    SELECT {", ".join(selected_fields)}
    FROM `{SACKMANN_PLAYER_SURFACE_FEATURES_TABLE}`
    WHERE player_name_norm IN UNNEST(@player_norms)
      AND {where_surface}
    """
    return _safe_query(sql, params)


def _fetch_sackmann_h2h_rows(
    *,
    home_norm: str,
    away_norm: str,
    surface_key: Optional[str],
) -> List[Dict[str, Any]]:
    if not home_norm or not away_norm:
        return []

    columns = _table_columns(SACKMANN_H2H_FEATURES_TABLE)
    required = {"player_name_norm", "opponent_name_norm", "surface_key"}
    if not required.issubset(columns):
        return []

    selected_fields = [
        field
        for field in [
            "player_name_norm",
            "player_name",
            "opponent_name_norm",
            "opponent_name",
            "surface_key",
            "matches_played",
            "wins",
            "losses",
            "win_rate",
            "aces_per_match",
            "double_faults_per_match",
            "avg_games_per_match",
            "avg_sets_per_match",
            "recent_h2h_matches",
            "updated_at",
        ]
        if field in columns
    ]
    if not selected_fields:
        return []

    surface_keys = ["all"]
    if surface_key:
        surface_keys.append(surface_key)

    sql = f"""
    SELECT {", ".join(selected_fields)}
    FROM `{SACKMANN_H2H_FEATURES_TABLE}`
    WHERE (
      (player_name_norm = @home_norm AND opponent_name_norm = @away_norm)
      OR (player_name_norm = @away_norm AND opponent_name_norm = @home_norm)
    )
      AND surface_key IN UNNEST(@surface_keys)
    """
    return _safe_query(
        sql,
        [
            bigquery.ScalarQueryParameter("home_norm", "STRING", home_norm),
            bigquery.ScalarQueryParameter("away_norm", "STRING", away_norm),
            bigquery.ArrayQueryParameter("surface_keys", "STRING", surface_keys),
        ],
    )


def _shape_sackmann_player_row(row: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not row:
        return None
    return {
        "player_name": row.get("player_name"),
        "surface_key": row.get("surface_key"),
        "matches_played": row.get("matches_played"),
        "wins": row.get("wins"),
        "losses": row.get("losses"),
        "win_rate": row.get("win_rate"),
        "aces_per_match": row.get("aces_per_match"),
        "double_faults_per_match": row.get("double_faults_per_match"),
        "avg_games_per_match": row.get("avg_games_per_match"),
        "avg_sets_per_match": row.get("avg_sets_per_match"),
        "recent_aces_l5_avg": row.get("recent_aces_l5_avg"),
        "recent_double_faults_l5_avg": row.get("recent_double_faults_l5_avg"),
        "recent_avg_games_l5": row.get("recent_avg_games_l5"),
        "recent_avg_sets_l5": row.get("recent_avg_sets_l5"),
        "recent_aces_by_match": _safe_json_load(row.get("recent_aces_by_match")) or [],
        "recent_double_faults_by_match": _safe_json_load(row.get("recent_double_faults_by_match")) or [],
        "recent_form_last10": row.get("recent_form_last10"),
        "updated_at": row.get("updated_at"),
    }


def _shape_sackmann_h2h_row(row: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not row:
        return None
    return {
        "surface_key": row.get("surface_key"),
        "matches_played": row.get("matches_played"),
        "wins": row.get("wins"),
        "losses": row.get("losses"),
        "win_rate": row.get("win_rate"),
        "aces_per_match": row.get("aces_per_match"),
        "double_faults_per_match": row.get("double_faults_per_match"),
        "avg_games_per_match": row.get("avg_games_per_match"),
        "avg_sets_per_match": row.get("avg_sets_per_match"),
        "recent_h2h_matches": _safe_json_load(row.get("recent_h2h_matches")) or [],
        "updated_at": row.get("updated_at"),
    }


def _build_sackmann_payload(
    *,
    home_team: Optional[str],
    away_team: Optional[str],
    surface: Optional[str],
) -> Optional[Dict[str, Any]]:
    home_norm = _normalize_name_norm(home_team)
    away_norm = _normalize_name_norm(away_team)
    if not home_norm or not away_norm:
        return None

    surface_key = _normalize_surface_key(surface)
    player_rows = _fetch_sackmann_player_surface_rows(
        [home_norm, away_norm],
        surface_key=surface_key,
    )
    by_norm: Dict[str, Dict[str, Any]] = {}
    for row in player_rows:
        norm = str(row.get("player_name_norm") or "")
        if norm and norm not in by_norm:
            by_norm[norm] = row

    h2h_rows = _fetch_sackmann_h2h_rows(
        home_norm=home_norm,
        away_norm=away_norm,
        surface_key=surface_key,
    )
    h2h_lookup: Dict[tuple[str, str, str], Dict[str, Any]] = {}
    for row in h2h_rows:
        key = (
            str(row.get("player_name_norm") or ""),
            str(row.get("opponent_name_norm") or ""),
            str(row.get("surface_key") or ""),
        )
        h2h_lookup[key] = row

    home_h2h_all = h2h_lookup.get((home_norm, away_norm, "all"))
    home_h2h_surface = h2h_lookup.get((home_norm, away_norm, surface_key or ""))

    payload = {
        "source_repo": SACKMANN_SOURCE_REPO,
        "surface_key": surface_key,
        "players": {
            "home": _shape_sackmann_player_row(by_norm.get(home_norm)),
            "away": _shape_sackmann_player_row(by_norm.get(away_norm)),
        },
        "head_to_head": {
            "all": _shape_sackmann_h2h_row(home_h2h_all),
            "surface": _shape_sackmann_h2h_row(home_h2h_surface),
        },
    }

    if not payload["players"]["home"] and not payload["players"]["away"] and not payload["head_to_head"]["all"]:
        return None
    return payload


def _build_matchup_payload(
    *,
    upcoming_row: Optional[Dict[str, Any]],
    weather_row: Optional[Dict[str, Any]],
    headshot_map: Dict[str, str],
) -> Dict[str, Any]:
    home_team = (upcoming_row or {}).get("home_team") or (weather_row or {}).get("home_team")
    away_team = (upcoming_row or {}).get("away_team") or (weather_row or {}).get("away_team")
    start_time_utc = (upcoming_row or {}).get("start_time_utc") or (weather_row or {}).get("date_utc")
    round_name = (weather_row or {}).get("round_name")
    tournament_name = (upcoming_row or {}).get("tournament_name")
    return {
        "home_team": home_team,
        "away_team": away_team,
        "matchup": (upcoming_row or {}).get("matchup") or (f"{home_team} vs {away_team}" if home_team and away_team else None),
        "start_time_utc": start_time_utc,
        "round_name": round_name,
        "tournament_name": tournament_name,
        "home_rank": (weather_row or {}).get("ht_rank"),
        "away_rank": (weather_row or {}).get("at_rank"),
        "home_headshot_url": headshot_map.get(str(home_team)) if home_team else None,
        "away_headshot_url": headshot_map.get(str(away_team)) if away_team else None,
    }


@router.get("/matchups/upcoming")
def atp_matchups_upcoming(
    limit: int = Query(default=50, ge=1, le=500),
    lookahead_days: int = Query(default=14, ge=1, le=60),
):
    rows = _fetch_upcoming_rows(limit=limit, lookahead_days=lookahead_days)
    if not rows:
        return []

    names: List[str] = []
    match_ids: List[int] = []
    for row in rows:
        if row.get("home_team"):
            names.append(str(row["home_team"]))
        if row.get("away_team"):
            names.append(str(row["away_team"]))
        if row.get("match_id") is not None:
            match_ids.append(int(row["match_id"]))

    headshots = _fetch_player_headshots(names)
    odds_summary_map = _fetch_odds_summary_for_matches(match_ids)
    payload: List[Dict[str, Any]] = []
    for row in rows:
        home_team = row.get("home_team")
        away_team = row.get("away_team")
        match_id = row.get("match_id")
        odds_summary = odds_summary_map.get(int(match_id)) if match_id is not None else None
        payload.append(
            {
                **row,
                "home_headshot_url": headshots.get(str(home_team)) if home_team else None,
                "away_headshot_url": headshots.get(str(away_team)) if away_team else None,
                "odds_summary": odds_summary,
            }
        )
    return payload


@router.get("/matchups/{match_id}")
def atp_matchup_detail(match_id: int):
    upcoming_row = _fetch_upcoming_match_row(match_id)
    weather_row = _fetch_match_weather_row(match_id)

    names = [
        str(name)
        for name in [
            (upcoming_row or {}).get("home_team") or (weather_row or {}).get("home_team"),
            (upcoming_row or {}).get("away_team") or (weather_row or {}).get("away_team"),
        ]
        if name
    ]
    headshots = _fetch_player_headshots(names)
    matchup = _build_matchup_payload(
        upcoming_row=upcoming_row,
        weather_row=weather_row,
        headshot_map=headshots,
    )

    odds_summary = _fetch_odds_summary_for_matches([match_id]).get(match_id)
    odds_board, odds_updated_at = _fetch_match_odds_board(match_id)
    h2h_summary = _fetch_head_to_head_summary_row(match_id)
    h2h_matches = _fetch_head_to_head_match_rows(match_id)
    sackmann_stats = _build_sackmann_payload(
        home_team=matchup.get("home_team"),
        away_team=matchup.get("away_team"),
        surface=(weather_row or {}).get("surface"),
    )

    return {
        "match_id": match_id,
        "matchup": matchup,
        "match_info": weather_row,
        "match_keys": _fetch_match_keys_rows(match_id),
        "betting_info": _fetch_betting_stats_rows(match_id),
        "head_to_head_summary": h2h_summary,
        "head_to_head_stats": h2h_matches,
        "head_to_head_matches": h2h_matches,
        "recent_matches": _fetch_recent_match_rows(match_id),
        "odds_summary": odds_summary,
        "odds_board": odds_board,
        "odds_updated_at": odds_updated_at,
        "sackmann_stats": sackmann_stats,
    }

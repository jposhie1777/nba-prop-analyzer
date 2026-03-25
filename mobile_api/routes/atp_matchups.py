from __future__ import annotations

import os
import re
import unicodedata
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
WEBSITE_MATCH_RESULTS_TABLE = os.getenv(
    "ATP_WEBSITE_MATCH_RESULTS_TABLE",
    "atp_data.website_match_results",
)
WEBSITE_HAWKEYE_MATCH_STATS_TABLE = os.getenv(
    "ATP_WEBSITE_HAWKEYE_MATCH_STATS_TABLE",
    "atp_data.website_hawkeye_match_stats",
)


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
    ascii_value = (
        unicodedata.normalize("NFKD", str(value))
        .encode("ascii", "ignore")
        .decode("ascii")
    )
    return "".join(ch for ch in ascii_value.lower().strip() if ch.isalnum())


def _strip_rank_annotations(value: Optional[str]) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", re.sub(r"\s*\([^)]*\)", "", str(value))).strip()


def _canonical_player_norm(value: Optional[str]) -> str:
    return _normalize_name_norm(_strip_rank_annotations(value))


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


def _score_set_values(score_text: Optional[str]) -> List[int]:
    if not score_text:
        return []
    values: List[int] = []
    for token in str(score_text).split():
        match = re.match(r"^(\d+)", token.strip())
        if not match:
            continue
        values.append(int(match.group(1)))
    return values


def _set_and_game_metrics(
    player_1_scores: Optional[str],
    player_2_scores: Optional[str],
) -> Dict[str, Optional[int]]:
    p1_values = _score_set_values(player_1_scores)
    p2_values = _score_set_values(player_2_scores)
    paired = min(len(p1_values), len(p2_values))
    if paired <= 0:
        return {
            "sets_played": None,
            "total_games": None,
            "player_1_sets_won": None,
            "player_2_sets_won": None,
        }

    p1_sets_won = 0
    p2_sets_won = 0
    for idx in range(paired):
        if p1_values[idx] > p2_values[idx]:
            p1_sets_won += 1
        elif p2_values[idx] > p1_values[idx]:
            p2_sets_won += 1

    return {
        "sets_played": paired,
        "total_games": sum(p1_values[:paired]) + sum(p2_values[:paired]),
        "player_1_sets_won": p1_sets_won,
        "player_2_sets_won": p2_sets_won,
    }


def _fetch_website_h2h_rows(home_norm: str, away_norm: str, *, limit: int) -> List[Dict[str, Any]]:
    if not home_norm or not away_norm:
        return []
    sql = f"""
    WITH base AS (
      SELECT
        match_date,
        tournament_slug,
        round_and_court,
        player_1_name,
        player_2_name,
        player_1_profile_url,
        player_2_profile_url,
        player_1_scores,
        player_2_scores,
        player_1_is_winner,
        player_2_is_winner,
        snapshot_ts_utc,
        REGEXP_REPLACE(LOWER(REGEXP_REPLACE(COALESCE(player_1_name, ''), r'\\s*\\([^)]*\\)', '')), r'[^a-z0-9]', '') AS player_1_norm,
        REGEXP_REPLACE(LOWER(REGEXP_REPLACE(COALESCE(player_2_name, ''), r'\\s*\\([^)]*\\)', '')), r'[^a-z0-9]', '') AS player_2_norm
      FROM `{WEBSITE_MATCH_RESULTS_TABLE}`
      WHERE match_date IS NOT NULL
    ),
    deduped AS (
      SELECT * EXCEPT (rn)
      FROM (
        SELECT
          *,
          ROW_NUMBER() OVER (
            PARTITION BY
              match_date,
              COALESCE(tournament_slug, ''),
              COALESCE(round_and_court, ''),
              COALESCE(player_1_profile_url, player_1_name, ''),
              COALESCE(player_2_profile_url, player_2_name, ''),
              COALESCE(player_1_scores, ''),
              COALESCE(player_2_scores, '')
            ORDER BY snapshot_ts_utc DESC
          ) AS rn
        FROM base
        WHERE (player_1_norm = @home_norm AND player_2_norm = @away_norm)
           OR (player_1_norm = @away_norm AND player_2_norm = @home_norm)
      )
      WHERE rn = 1
    )
    SELECT
      match_date,
      tournament_slug,
      round_and_court,
      player_1_name,
      player_2_name,
      player_1_scores,
      player_2_scores,
      player_1_is_winner,
      player_2_is_winner,
      player_1_norm,
      player_2_norm
    FROM deduped
    ORDER BY match_date DESC
    LIMIT @limit
    """
    return _safe_query(
        sql,
        [
            bigquery.ScalarQueryParameter("home_norm", "STRING", home_norm),
            bigquery.ScalarQueryParameter("away_norm", "STRING", away_norm),
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
        ],
    )


def _fetch_website_h2h_summary(home_norm: str, away_norm: str) -> Optional[Dict[str, Any]]:
    if not home_norm or not away_norm:
        return None
    sql = f"""
    WITH base AS (
      SELECT
        match_date,
        tournament_slug,
        round_and_court,
        player_1_name,
        player_2_name,
        player_1_profile_url,
        player_2_profile_url,
        player_1_scores,
        player_2_scores,
        player_1_is_winner,
        player_2_is_winner,
        snapshot_ts_utc,
        REGEXP_REPLACE(LOWER(REGEXP_REPLACE(COALESCE(player_1_name, ''), r'\\s*\\([^)]*\\)', '')), r'[^a-z0-9]', '') AS player_1_norm,
        REGEXP_REPLACE(LOWER(REGEXP_REPLACE(COALESCE(player_2_name, ''), r'\\s*\\([^)]*\\)', '')), r'[^a-z0-9]', '') AS player_2_norm
      FROM `{WEBSITE_MATCH_RESULTS_TABLE}`
      WHERE match_date IS NOT NULL
    ),
    deduped AS (
      SELECT * EXCEPT (rn)
      FROM (
        SELECT
          *,
          ROW_NUMBER() OVER (
            PARTITION BY
              match_date,
              COALESCE(tournament_slug, ''),
              COALESCE(round_and_court, ''),
              COALESCE(player_1_profile_url, player_1_name, ''),
              COALESCE(player_2_profile_url, player_2_name, ''),
              COALESCE(player_1_scores, ''),
              COALESCE(player_2_scores, '')
            ORDER BY snapshot_ts_utc DESC
          ) AS rn
        FROM base
        WHERE (player_1_norm = @home_norm AND player_2_norm = @away_norm)
           OR (player_1_norm = @away_norm AND player_2_norm = @home_norm)
      )
      WHERE rn = 1
    )
    SELECT
      COUNT(1) AS matches_played,
      SUM(
        CASE
          WHEN (player_1_norm = @home_norm AND player_1_is_winner)
            OR (player_2_norm = @home_norm AND player_2_is_winner)
          THEN 1 ELSE 0
        END
      ) AS home_wins,
      SUM(
        CASE
          WHEN (player_1_norm = @away_norm AND player_1_is_winner)
            OR (player_2_norm = @away_norm AND player_2_is_winner)
          THEN 1 ELSE 0
        END
      ) AS away_wins,
      MIN(EXTRACT(YEAR FROM match_date)) AS first_year,
      MAX(EXTRACT(YEAR FROM match_date)) AS last_year
    FROM deduped
    """
    rows = _safe_query(
        sql,
        [
            bigquery.ScalarQueryParameter("home_norm", "STRING", home_norm),
            bigquery.ScalarQueryParameter("away_norm", "STRING", away_norm),
        ],
    )
    if not rows:
        return None
    row = rows[0]
    played = int(row.get("matches_played") or 0)
    if played <= 0:
        return None
    first_year = row.get("first_year")
    last_year = row.get("last_year")
    period_years = None
    if first_year and last_year:
        period_years = str(first_year) if first_year == last_year else f"{first_year}-{last_year}"
    return {
        "ht_wins": int(row.get("home_wins") or 0),
        "at_wins": int(row.get("away_wins") or 0),
        "draws": 0,
        "played_matches": played,
        "period_years": period_years,
    }


def _fetch_website_player_rows(player_norm: str, *, limit: int) -> List[Dict[str, Any]]:
    if not player_norm:
        return []
    sql = f"""
    WITH base AS (
      SELECT
        match_date,
        tournament_slug,
        round_and_court,
        player_1_name,
        player_2_name,
        player_1_profile_url,
        player_2_profile_url,
        player_1_scores,
        player_2_scores,
        player_1_is_winner,
        player_2_is_winner,
        snapshot_ts_utc,
        REGEXP_REPLACE(LOWER(REGEXP_REPLACE(COALESCE(player_1_name, ''), r'\\s*\\([^)]*\\)', '')), r'[^a-z0-9]', '') AS player_1_norm,
        REGEXP_REPLACE(LOWER(REGEXP_REPLACE(COALESCE(player_2_name, ''), r'\\s*\\([^)]*\\)', '')), r'[^a-z0-9]', '') AS player_2_norm
      FROM `{WEBSITE_MATCH_RESULTS_TABLE}`
      WHERE match_date IS NOT NULL
    ),
    deduped AS (
      SELECT * EXCEPT (rn)
      FROM (
        SELECT
          *,
          ROW_NUMBER() OVER (
            PARTITION BY
              match_date,
              COALESCE(tournament_slug, ''),
              COALESCE(round_and_court, ''),
              COALESCE(player_1_profile_url, player_1_name, ''),
              COALESCE(player_2_profile_url, player_2_name, ''),
              COALESCE(player_1_scores, ''),
              COALESCE(player_2_scores, '')
            ORDER BY snapshot_ts_utc DESC
          ) AS rn
        FROM base
        WHERE player_1_norm = @player_norm
           OR player_2_norm = @player_norm
      )
      WHERE rn = 1
    )
    SELECT
      match_date,
      tournament_slug,
      round_and_court,
      player_1_name,
      player_2_name,
      player_1_scores,
      player_2_scores,
      player_1_is_winner,
      player_2_is_winner,
      player_1_norm,
      player_2_norm
    FROM deduped
    ORDER BY match_date DESC
    LIMIT @limit
    """
    return _safe_query(
        sql,
        [
            bigquery.ScalarQueryParameter("player_norm", "STRING", player_norm),
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
        ],
    )


def _result_for_player(row: Dict[str, Any], player_norm: str) -> Optional[bool]:
    if str(row.get("player_1_norm") or "") == player_norm:
        value = row.get("player_1_is_winner")
        return bool(value) if value is not None else None
    if str(row.get("player_2_norm") or "") == player_norm:
        value = row.get("player_2_is_winner")
        return bool(value) if value is not None else None
    return None


def _name_for_player(row: Dict[str, Any], player_norm: str) -> Optional[str]:
    if str(row.get("player_1_norm") or "") == player_norm:
        return _strip_rank_annotations(row.get("player_1_name"))
    if str(row.get("player_2_norm") or "") == player_norm:
        return _strip_rank_annotations(row.get("player_2_name"))
    return None


def _opponent_name_for_player(row: Dict[str, Any], player_norm: str) -> Optional[str]:
    if str(row.get("player_1_norm") or "") == player_norm:
        return _strip_rank_annotations(row.get("player_2_name"))
    if str(row.get("player_2_norm") or "") == player_norm:
        return _strip_rank_annotations(row.get("player_1_name"))
    return None


def _recent_record(rows: Sequence[Dict[str, Any]], player_norm: str, count: int) -> Dict[str, Any]:
    sample = list(rows[:count])
    sequence: List[str] = []
    wins = 0
    losses = 0
    resolved = 0
    for row in sample:
        won = _result_for_player(row, player_norm)
        if won is None:
            continue
        resolved += 1
        if won:
            wins += 1
            sequence.append("W")
        else:
            losses += 1
            sequence.append("L")
    return {
        "matches": resolved,
        "wins": wins,
        "losses": losses,
        "record": f"{wins}-{losses}",
        "sequence": "".join(sequence),
    }


def _recent_averages(rows: Sequence[Dict[str, Any]], count: int) -> Dict[str, Optional[float]]:
    sample = list(rows[:count])
    total_sets = 0
    total_games = 0
    valid_count = 0
    for row in sample:
        metrics = _set_and_game_metrics(row.get("player_1_scores"), row.get("player_2_scores"))
        sets_played = metrics.get("sets_played")
        games = metrics.get("total_games")
        if sets_played is None or games is None:
            continue
        total_sets += int(sets_played)
        total_games += int(games)
        valid_count += 1
    if valid_count <= 0:
        return {"avg_sets": None, "avg_total_games": None}
    return {
        "avg_sets": round(total_sets / valid_count, 2),
        "avg_total_games": round(total_games / valid_count, 2),
    }


def _shape_h2h_rows(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    payload: List[Dict[str, Any]] = []
    for row in rows:
        metrics = _set_and_game_metrics(row.get("player_1_scores"), row.get("player_2_scores"))
        winner_name = None
        if row.get("player_1_is_winner"):
            winner_name = _strip_rank_annotations(row.get("player_1_name"))
        elif row.get("player_2_is_winner"):
            winner_name = _strip_rank_annotations(row.get("player_2_name"))
        payload.append(
            {
                "h2h_starttime": row.get("match_date"),
                "h2h_ht": _strip_rank_annotations(row.get("player_1_name")),
                "h2h_at": _strip_rank_annotations(row.get("player_2_name")),
                "h2h_hscore": metrics.get("player_1_sets_won"),
                "h2h_ascore": metrics.get("player_2_sets_won"),
                "h2h_winner": winner_name,
                "h2h_league_name": row.get("tournament_slug"),
                "h2h_round": row.get("round_and_court"),
                "h2h_total_games": metrics.get("total_games"),
                "h2h_sets_played": metrics.get("sets_played"),
            }
        )
    return payload


def _shape_recent_rows_for_player(
    rows: Sequence[Dict[str, Any]],
    *,
    side: str,
    player_norm: str,
) -> List[Dict[str, Any]]:
    payload: List[Dict[str, Any]] = []
    for row in rows:
        self_name = _name_for_player(row, player_norm)
        opponent = _opponent_name_for_player(row, player_norm)
        won = _result_for_player(row, player_norm)
        metrics = _set_and_game_metrics(row.get("player_1_scores"), row.get("player_2_scores"))
        if str(row.get("player_1_norm") or "") == player_norm:
            sets_for = metrics.get("player_1_sets_won")
            sets_against = metrics.get("player_2_sets_won")
        elif str(row.get("player_2_norm") or "") == player_norm:
            sets_for = metrics.get("player_2_sets_won")
            sets_against = metrics.get("player_1_sets_won")
        else:
            sets_for = None
            sets_against = None
        payload.append(
            {
                "side": side,
                "last_starttime": row.get("match_date"),
                "last_ht": self_name,
                "last_at": opponent,
                "last_hscore": sets_for,
                "last_ascore": sets_against,
                "last_outcome": "W" if won else ("L" if won is not None else None),
                "tournament_name": row.get("tournament_slug"),
                "round_name": row.get("round_and_court"),
                "total_games": metrics.get("total_games"),
                "sets_played": metrics.get("sets_played"),
            }
        )
    return payload


def _build_player_history_payload(
    *,
    player_name: Optional[str],
    player_norm: str,
    rows: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    display_name = _strip_rank_annotations(player_name) or None
    if not display_name and rows:
        display_name = _name_for_player(rows[0], player_norm)
    return {
        "player_name": display_name,
        "recent_form": {
            "l5": _recent_record(rows, player_norm, 5),
            "l10": _recent_record(rows, player_norm, 10),
        },
        "averages": {
            "l5": _recent_averages(rows, 5),
            "l10": _recent_averages(rows, 10),
        },
    }


def _build_website_match_history_payload(
    *,
    home_team: Optional[str],
    away_team: Optional[str],
) -> Dict[str, Any]:
    home_norm = _canonical_player_norm(home_team)
    away_norm = _canonical_player_norm(away_team)
    if not home_norm or not away_norm:
        return {
            "head_to_head_summary": None,
            "head_to_head_rows": [],
            "player_match_history": None,
            "recent_matches": [],
        }

    h2h_rows = _fetch_website_h2h_rows(home_norm, away_norm, limit=30)
    h2h_summary = _fetch_website_h2h_summary(home_norm, away_norm)
    home_rows = _fetch_website_player_rows(home_norm, limit=12)
    away_rows = _fetch_website_player_rows(away_norm, limit=12)

    return {
        "head_to_head_summary": h2h_summary,
        "head_to_head_rows": _shape_h2h_rows(h2h_rows),
        "player_match_history": {
            "home": _build_player_history_payload(
                player_name=home_team,
                player_norm=home_norm,
                rows=home_rows,
            ),
            "away": _build_player_history_payload(
                player_name=away_team,
                player_norm=away_norm,
                rows=away_rows,
            ),
        },
        "recent_matches": [
            *_shape_recent_rows_for_player(home_rows[:10], side="home", player_norm=home_norm),
            *_shape_recent_rows_for_player(away_rows[:10], side="away", player_norm=away_norm),
        ],
    }


def _fetch_hawkeye_player_match_rows(
    player_name: Optional[str],
    player_norm: str,
    *,
    limit: int,
) -> List[Dict[str, Any]]:
    if not player_name and not player_norm:
        return []

    sql = f"""
    WITH base AS (
      SELECT
        match_date,
        tournament_name,
        round_name,
        player_name,
        opponent_name,
        stats_url,
        snapshot_ts_utc,
        service_games_played,
        aces,
        double_faults,
        first_serve_pts_won_pct,
        second_serve_pts_won_pct,
        first_serve_return_pts_won_pct,
        second_serve_return_pts_won_pct,
        REGEXP_REPLACE(LOWER(REGEXP_REPLACE(COALESCE(player_name, ''), r'\\s*\\([^)]*\\)', '')), r'[^a-z0-9]', '') AS player_norm
      FROM `{WEBSITE_HAWKEYE_MATCH_STATS_TABLE}`
      WHERE match_date IS NOT NULL
        AND set_number = 0
    ),
    filtered AS (
      SELECT *
      FROM base
      WHERE (
        @player_norm != ''
        AND player_norm = @player_norm
      )
      OR (
        @player_name != ''
        AND LOWER(TRIM(player_name)) = LOWER(TRIM(@player_name))
      )
    ),
    deduped AS (
      SELECT * EXCEPT (rn)
      FROM (
        SELECT
          *,
          ROW_NUMBER() OVER (
            PARTITION BY
              match_date,
              COALESCE(stats_url, ''),
              COALESCE(player_name, ''),
              COALESCE(opponent_name, '')
            ORDER BY snapshot_ts_utc DESC
          ) AS rn
        FROM filtered
      )
      WHERE rn = 1
    )
    SELECT
      match_date,
      tournament_name,
      round_name,
      player_name,
      opponent_name,
      service_games_played,
      aces,
      double_faults,
      first_serve_pts_won_pct,
      second_serve_pts_won_pct,
      first_serve_return_pts_won_pct,
      second_serve_return_pts_won_pct
    FROM deduped
    ORDER BY match_date DESC
    LIMIT @limit
    """
    return _safe_query(
        sql,
        [
            bigquery.ScalarQueryParameter("player_norm", "STRING", player_norm),
            bigquery.ScalarQueryParameter("player_name", "STRING", _strip_rank_annotations(player_name)),
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
        ],
    )


def _window_metric_avg(rows: Sequence[Dict[str, Any]], key: str, count: int) -> Dict[str, Optional[float]]:
    sample = list(rows[:count])
    values: List[float] = []
    for row in sample:
        value = row.get(key)
        if value is None:
            continue
        try:
            values.append(float(value))
        except (TypeError, ValueError):
            continue
    if not values:
        return {"matches": 0, "value": None}
    return {"matches": len(values), "value": round(sum(values) / len(values), 2)}


def _window_per_game_avg(
    rows: Sequence[Dict[str, Any]],
    numerator_key: str,
    denominator_key: str,
    count: int,
) -> Dict[str, Optional[float]]:
    sample = list(rows[:count])
    numer_sum = 0.0
    denom_sum = 0.0
    used = 0
    for row in sample:
        numer = row.get(numerator_key)
        denom = row.get(denominator_key)
        if numer is None or denom is None:
            continue
        try:
            numer_value = float(numer)
            denom_value = float(denom)
        except (TypeError, ValueError):
            continue
        if denom_value <= 0:
            continue
        numer_sum += numer_value
        denom_sum += denom_value
        used += 1
    if used <= 0 or denom_sum <= 0:
        return {"matches": 0, "value": None}
    return {"matches": used, "value": round(numer_sum / denom_sum, 3)}


def _build_player_stats_analysis_payload(
    *,
    player_name: Optional[str],
    rows: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    display_name = _strip_rank_annotations(player_name) or None
    if not display_name and rows:
        display_name = _strip_rank_annotations(rows[0].get("player_name"))
    windows = [5, 10, 20]
    analysis: Dict[str, Any] = {}
    for window in windows:
        analysis[f"l{window}"] = {
            "aces_per_match": _window_metric_avg(rows, "aces", window),
            "double_faults_per_match": _window_metric_avg(rows, "double_faults", window),
            "first_serve_won_pct": _window_metric_avg(rows, "first_serve_pts_won_pct", window),
            "second_serve_won_pct": _window_metric_avg(rows, "second_serve_pts_won_pct", window),
            "first_serve_return_won_pct": _window_metric_avg(rows, "first_serve_return_pts_won_pct", window),
            "second_serve_return_won_pct": _window_metric_avg(rows, "second_serve_return_pts_won_pct", window),
        }
    return {
        "player_name": display_name,
        "windows": analysis,
        "recent_matches": [
            {
                "match_date": row.get("match_date"),
                "tournament_name": row.get("tournament_name"),
                "round_name": row.get("round_name"),
                "opponent_name": row.get("opponent_name"),
                "service_games_played": row.get("service_games_played"),
                "aces": row.get("aces"),
                "double_faults": row.get("double_faults"),
                "first_serve_won_pct": row.get("first_serve_pts_won_pct"),
                "second_serve_won_pct": row.get("second_serve_pts_won_pct"),
                "first_serve_return_won_pct": row.get("first_serve_return_pts_won_pct"),
                "second_serve_return_won_pct": row.get("second_serve_return_pts_won_pct"),
            }
            for row in rows[:20]
        ],
    }


def _build_hawkeye_player_analysis_payload(
    *,
    home_team: Optional[str],
    away_team: Optional[str],
) -> Dict[str, Any]:
    home_norm = _canonical_player_norm(home_team)
    away_norm = _canonical_player_norm(away_team)
    if not home_norm or not away_norm:
        return {"home": None, "away": None}

    home_rows = _fetch_hawkeye_player_match_rows(home_team, home_norm, limit=25)
    away_rows = _fetch_hawkeye_player_match_rows(away_team, away_norm, limit=25)

    return {
        "home": _build_player_stats_analysis_payload(
            player_name=home_team,
            rows=home_rows,
        ),
        "away": _build_player_stats_analysis_payload(
            player_name=away_team,
            rows=away_rows,
        ),
    }


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
    website_payload = _build_website_match_history_payload(
        home_team=matchup.get("home_team"),
        away_team=matchup.get("away_team"),
    )
    has_matchup_players = bool(
        _canonical_player_norm(matchup.get("home_team")) and _canonical_player_norm(matchup.get("away_team"))
    )
    if has_matchup_players:
        h2h_summary = website_payload.get("head_to_head_summary")
        h2h_matches = website_payload.get("head_to_head_rows") or []
        recent_matches = website_payload.get("recent_matches") or []
    else:
        # Fallback only when matchup names are missing and website lookups cannot be performed.
        h2h_summary = _fetch_head_to_head_summary_row(match_id)
        h2h_matches = _fetch_head_to_head_match_rows(match_id)
        recent_matches = _fetch_recent_match_rows(match_id)

    return {
        "match_id": match_id,
        "matchup": matchup,
        "match_info": weather_row,
        "match_keys": _fetch_match_keys_rows(match_id),
        "betting_info": _fetch_betting_stats_rows(match_id),
        "head_to_head_summary": h2h_summary,
        "head_to_head_stats": h2h_matches,
        "head_to_head_matches": h2h_matches,
        "recent_matches": recent_matches,
        "player_match_history": website_payload.get("player_match_history"),
        "player_stats_analysis": _build_hawkeye_player_analysis_payload(
            home_team=matchup.get("home_team"),
            away_team=matchup.get("away_team"),
        ),
        "odds_summary": odds_summary,
        "odds_board": odds_board,
        "odds_updated_at": odds_updated_at,
    }

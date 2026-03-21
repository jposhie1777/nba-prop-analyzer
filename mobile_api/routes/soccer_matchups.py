from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Sequence, Tuple

from fastapi import APIRouter, Query
from google.api_core.exceptions import BadRequest, NotFound
from google.cloud import bigquery

from bq import get_bq_client

router = APIRouter(tags=["Soccer Matchups"])


LEAGUE_TABLES: Dict[str, Dict[str, str]] = {
    "epl": {
        "upcoming": os.getenv("ODDSPEDIA_EPL_UPCOMING_MATCHES_TABLE", "oddspedia.epl_upcoming_matches"),
        "match_info": os.getenv("ODDSPEDIA_EPL_MATCH_INFO_TABLE", "oddspedia.epl_match_info"),
        "match_info_fallback": os.getenv("ODDSPEDIA_EPL_MATCH_WEATHER_TABLE", "oddspedia.epl_match_weather"),
        "match_keys": os.getenv("ODDSPEDIA_EPL_MATCH_KEYS_TABLE", "oddspedia.epl_match_keys"),
        "betting_stats": os.getenv("ODDSPEDIA_EPL_BETTING_STATS_TABLE", "oddspedia.epl_betting_stats"),
        "last_matches": os.getenv("ODDSPEDIA_EPL_LAST_MATCHES_TABLE", "oddspedia.epl_last_matches"),
        "odds": os.getenv("ODDSPEDIA_EPL_ODDS_TABLE", "oddspedia.epl_odds"),
    },
    "mls": {
        "upcoming": os.getenv("ODDSPEDIA_MLS_UPCOMING_MATCHES_TABLE", "oddspedia.mls_upcoming_matches"),
        "match_info": os.getenv("ODDSPEDIA_MLS_MATCH_INFO_TABLE", "oddspedia.mls_match_info"),
        "match_info_fallback": os.getenv("ODDSPEDIA_MLS_MATCH_WEATHER_TABLE", "oddspedia.mls_match_weather"),
        "match_keys": os.getenv("ODDSPEDIA_MLS_MATCH_KEYS_TABLE", "oddspedia.mls_match_keys"),
        "betting_stats": os.getenv("ODDSPEDIA_MLS_BETTING_STATS_TABLE", "oddspedia.mls_betting_stats"),
        "last_matches": os.getenv("ODDSPEDIA_MLS_LAST_MATCHES_TABLE", "oddspedia.mls_last_matches"),
        "odds": os.getenv("ODDSPEDIA_MLS_ODDS_TABLE", "oddspedia.mls_odds"),
    },
}


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
    return [dict(r) for r in job.result()]


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
    rows = _safe_query(
        sql,
        [bigquery.ScalarQueryParameter("table", "STRING", table)],
    )
    return {r.get("column_name") for r in rows if r.get("column_name")}


def _query_upcoming_from_table(
    table_ref: str,
    *,
    start_time_column: str,
    lookahead_days: int,
    limit: int,
) -> List[Dict[str, Any]]:
    columns = _table_columns(table_ref)
    required = {"match_id", "home_team", "away_team", start_time_column}
    if not required.issubset(columns):
        return []

    matchup_select = "matchup" if "matchup" in columns else "CONCAT(home_team, ' vs ', away_team) AS matchup"
    ingested_select = "ingested_at" if "ingested_at" in columns else "CAST(NULL AS TIMESTAMP) AS ingested_at"

    sql = f"""
    WITH normalized AS (
      SELECT
        CAST(match_id AS INT64) AS match_id,
        home_team,
        away_team,
        {matchup_select},
        SAFE_CAST({start_time_column} AS TIMESTAMP) AS start_time_utc,
        {ingested_select}
      FROM `{table_ref}`
    ),
    deduped AS (
      SELECT
        match_id,
        home_team,
        away_team,
        matchup,
        start_time_utc,
        ingested_at
      FROM normalized
      WHERE match_id IS NOT NULL
        AND start_time_utc IS NOT NULL
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY match_id
        ORDER BY ingested_at DESC NULLS LAST, start_time_utc ASC
      ) = 1
    )
    SELECT
      match_id,
      home_team,
      away_team,
      COALESCE(NULLIF(matchup, ''), CONCAT(home_team, ' vs ', away_team)) AS matchup,
      start_time_utc
    FROM deduped
    WHERE DATE(start_time_utc) BETWEEN CURRENT_DATE() AND DATE_ADD(CURRENT_DATE(), INTERVAL @lookahead_days DAY)
    ORDER BY start_time_utc ASC, matchup ASC
    LIMIT @limit
    """
    return _safe_query(
        sql,
        [
            bigquery.ScalarQueryParameter("lookahead_days", "INT64", lookahead_days),
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
        ],
    )


def _fetch_recent_form_map(table_ref: str, match_ids: Sequence[int]) -> Dict[int, Dict[str, Optional[str]]]:
    if not match_ids:
        return {}

    columns = _table_columns(table_ref)
    if "match_id" not in columns:
        return {}
    if "home_form" not in columns and "away_form" not in columns:
        return {}

    home_form_select = "home_form" if "home_form" in columns else "CAST(NULL AS STRING) AS home_form"
    away_form_select = "away_form" if "away_form" in columns else "CAST(NULL AS STRING) AS away_form"
    order_sql = _order_sql_for_columns(columns)

    sql = f"""
    WITH latest AS (
      SELECT
        CAST(match_id AS INT64) AS match_id,
        {home_form_select},
        {away_form_select},
        ROW_NUMBER() OVER (
          PARTITION BY match_id
          ORDER BY {order_sql}
        ) AS rn
      FROM `{table_ref}`
      WHERE CAST(match_id AS INT64) IN UNNEST(@match_ids)
    )
    SELECT match_id, home_form, away_form
    FROM latest
    WHERE rn = 1
    """
    rows = _safe_query(
        sql,
        [bigquery.ArrayQueryParameter("match_ids", "INT64", list(match_ids))],
    )
    out: Dict[int, Dict[str, Optional[str]]] = {}
    for row in rows:
        match_id = row.get("match_id")
        if match_id is None:
            continue
        out[int(match_id)] = {
            "home_recent_form": row.get("home_form"),
            "away_recent_form": row.get("away_form"),
        }
    return out


def _order_sql_for_columns(columns: set[str]) -> str:
    order_parts: List[str] = []
    if "ingested_at" in columns:
        order_parts.append("ingested_at DESC")
    if "date_utc" in columns:
        order_parts.append("SAFE_CAST(date_utc AS TIMESTAMP) DESC")
    if "start_time_utc" in columns:
        order_parts.append("SAFE_CAST(start_time_utc AS TIMESTAMP) DESC")
    if "rank" in columns:
        order_parts.append("rank ASC")
    if "lm_date" in columns:
        order_parts.append("SAFE_CAST(lm_date AS TIMESTAMP) DESC")
    return ", ".join(order_parts) if order_parts else "match_id DESC"


def _normalize_outcome_key(value: Any) -> Optional[str]:
    if value is None:
        return None
    normalized = str(value).strip().lower()
    if not normalized:
        return None
    if normalized in {"home", "o1", "1"}:
        return "home"
    if normalized in {"draw", "tie", "x", "o2"}:
        return "draw"
    if normalized in {"away", "o3", "2"}:
        return "away"
    return None


def _fetch_odds_summary_for_matches(table_ref: str, match_ids: Sequence[int]) -> Dict[int, Dict[str, Any]]:
    if not match_ids:
        return {}

    columns = _table_columns(table_ref)
    if "match_id" not in columns:
        return {}

    outcome_parts: List[str] = []
    for field in ("outcome_name", "outcome_key", "outcome_side"):
        if field in columns:
            outcome_parts.append(f"NULLIF(TRIM(CAST({field} AS STRING)), '')")
    if not outcome_parts:
        return {}

    market_parts: List[str] = []
    for field in ("market_group_name", "market"):
        if field in columns:
            market_parts.append(f"NULLIF(TRIM(CAST({field} AS STRING)), '')")
    if not market_parts:
        return {}

    outcome_expr = f"COALESCE({', '.join(outcome_parts)})"
    market_expr = f"LOWER(COALESCE({', '.join(market_parts)}))"
    bookie_expr = "NULLIF(TRIM(CAST(bookie AS STRING)), '')" if "bookie" in columns else "CAST(NULL AS STRING)"
    odds_decimal_expr = (
        "SAFE_CAST(odds_decimal AS FLOAT64)" if "odds_decimal" in columns else "CAST(NULL AS FLOAT64)"
    )
    odds_american_expr = (
        "SAFE_CAST(odds_american AS INT64)" if "odds_american" in columns else "CAST(NULL AS INT64)"
    )
    ingested_expr = (
        "SAFE_CAST(ingested_at AS TIMESTAMP)"
        if "ingested_at" in columns
        else "SAFE_CAST(date_utc AS TIMESTAMP)"
        if "date_utc" in columns
        else "CAST(NULL AS TIMESTAMP)"
    )
    period_filter = "TRUE"
    if "period_id" in columns:
        period_filter = "SAFE_CAST(period_id AS INT64) = 100"
    elif "period_name" in columns:
        period_filter = "LOWER(NULLIF(TRIM(CAST(period_name AS STRING)), '')) IN ('final', 'full time', 'fulltime')"

    sql = f"""
    WITH latest AS (
      SELECT
        CAST(match_id AS INT64) AS match_id,
        {outcome_expr} AS outcome_name,
        {bookie_expr} AS bookie,
        {odds_decimal_expr} AS odds_decimal,
        {odds_american_expr} AS odds_american,
        {ingested_expr} AS ingested_at,
        ROW_NUMBER() OVER (
          PARTITION BY
            CAST(match_id AS INT64),
            {outcome_expr},
            COALESCE({bookie_expr}, '')
          ORDER BY {ingested_expr} DESC NULLS LAST
        ) AS rn
      FROM `{table_ref}`
      WHERE CAST(match_id AS INT64) IN UNNEST(@match_ids)
        AND {market_expr} IN ('1x2', 'h2h', 'moneyline', 'match_winner', 'winner', 'outright_winner')
        AND {period_filter}
        AND {outcome_expr} IS NOT NULL
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
          PARTITION BY match_id, LOWER(outcome_name)
          ORDER BY odds_decimal DESC NULLS LAST, ingested_at DESC NULLS LAST
        ) AS best_rank
      FROM deduped
    )
    SELECT
      match_id,
      outcome_name,
      bookie,
      odds_decimal,
      odds_american,
      ingested_at
    FROM ranked
    WHERE best_rank = 1
    """

    rows = _safe_query(
        sql,
        [bigquery.ArrayQueryParameter("match_ids", "INT64", list(match_ids))],
    )

    out: Dict[int, Dict[str, Any]] = {}
    for row in rows:
        match_id = row.get("match_id")
        if match_id is None:
            continue
        key = _normalize_outcome_key(row.get("outcome_name"))
        if key is None:
            continue
        if int(match_id) not in out:
            out[int(match_id)] = {"home": None, "draw": None, "away": None, "updated_at": None}
        out[int(match_id)][key] = {
            "bookie": row.get("bookie"),
            "odds_decimal": row.get("odds_decimal"),
            "odds_american": row.get("odds_american"),
        }
        updated_at = row.get("ingested_at")
        current = out[int(match_id)].get("updated_at")
        if updated_at is not None and (current is None or updated_at > current):
            out[int(match_id)]["updated_at"] = updated_at
    return out


def _fetch_match_odds_board(
    table_ref: str,
    match_id: int,
    *,
    max_rows: int = 300,
    top_books_per_outcome: int = 2,
) -> Tuple[List[Dict[str, Any]], Optional[Any]]:
    columns = _table_columns(table_ref)
    if "match_id" not in columns:
        return [], None

    market_group_expr = (
        "NULLIF(TRIM(CAST(market_group_name AS STRING)), '')"
        if "market_group_name" in columns
        else "CAST(NULL AS STRING)"
    )
    market_expr = (
        "NULLIF(TRIM(CAST(market AS STRING)), '')"
        if "market" in columns
        else "CAST(NULL AS STRING)"
    )
    outcome_parts: List[str] = []
    for field in ("outcome_name", "outcome_key", "outcome_side"):
        if field in columns:
            outcome_parts.append(f"NULLIF(TRIM(CAST({field} AS STRING)), '')")
    if not outcome_parts:
        return [], None
    outcome_expr = f"COALESCE({', '.join(outcome_parts)})"

    period_id_expr = (
        "SAFE_CAST(period_id AS INT64)" if "period_id" in columns else "CAST(NULL AS INT64)"
    )
    period_name_expr = (
        "NULLIF(TRIM(CAST(period_name AS STRING)), '')"
        if "period_name" in columns
        else "CAST(NULL AS STRING)"
    )
    line_expr = (
        "NULLIF(TRIM(CAST(line_value AS STRING)), '')"
        if "line_value" in columns
        else "CAST(NULL AS STRING)"
    )
    bookie_expr = (
        "NULLIF(TRIM(CAST(bookie AS STRING)), '')"
        if "bookie" in columns
        else "CAST(NULL AS STRING)"
    )
    odds_decimal_expr = (
        "SAFE_CAST(odds_decimal AS FLOAT64)" if "odds_decimal" in columns else "CAST(NULL AS FLOAT64)"
    )
    odds_american_expr = (
        "SAFE_CAST(odds_american AS INT64)" if "odds_american" in columns else "CAST(NULL AS INT64)"
    )
    ingested_expr = (
        "SAFE_CAST(ingested_at AS TIMESTAMP)"
        if "ingested_at" in columns
        else "SAFE_CAST(date_utc AS TIMESTAMP)"
        if "date_utc" in columns
        else "CAST(NULL AS TIMESTAMP)"
    )

    sql = f"""
    WITH latest AS (
      SELECT
        CAST(match_id AS INT64) AS match_id,
        COALESCE({market_group_expr}, {market_expr}, 'Other') AS market_group,
        COALESCE({market_expr}, {market_group_expr}, 'other') AS market,
        {period_id_expr} AS period_id,
        {period_name_expr} AS period_name,
        {line_expr} AS line_value,
        {outcome_expr} AS outcome_name,
        {bookie_expr} AS bookie,
        {odds_decimal_expr} AS odds_decimal,
        {odds_american_expr} AS odds_american,
        {ingested_expr} AS ingested_at,
        ROW_NUMBER() OVER (
          PARTITION BY
            CAST(match_id AS INT64),
            COALESCE({market_group_expr}, {market_expr}, 'Other'),
            COALESCE({market_expr}, {market_group_expr}, 'other'),
            COALESCE(CAST({period_id_expr} AS STRING), ''),
            COALESCE({period_name_expr}, ''),
            COALESCE({line_expr}, ''),
            {outcome_expr},
            COALESCE({bookie_expr}, '')
          ORDER BY {ingested_expr} DESC NULLS LAST
        ) AS rn
      FROM `{table_ref}`
      WHERE CAST(match_id AS INT64) = @match_id
        AND {outcome_expr} IS NOT NULL
        AND LOWER(COALESCE({market_expr}, {market_group_expr}, '')) NOT IN ('correct_score')
        AND LOWER(COALESCE({market_group_expr}, {market_expr}, '')) NOT IN ('correct score')
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
    latest_ingested = None
    for row in rows:
        ingested = row.get("ingested_at")
        if ingested is not None and (latest_ingested is None or ingested > latest_ingested):
            latest_ingested = ingested
    return rows, latest_ingested


def _fetch_match_info_row(table_ref: str, match_id: int) -> Optional[Dict[str, Any]]:
    columns = _table_columns(table_ref)
    if "match_id" not in columns:
        return None

    selected_fields: List[str] = []
    for field in [
        "match_id",
        "match_key",
        "home_team",
        "away_team",
        "date_utc",
        "round_name",
        "weather_icon",
        "weather_temp_c",
        "home_form",
        "away_form",
        "referee_name",
        "venue_name",
        "venue_city",
        "venue_capacity",
        "ingested_at",
        "scraped_date",
    ]:
        if field in columns:
            selected_fields.append(field)

    if not selected_fields:
        return None

    select_sql = ", ".join(selected_fields)
    order_sql = _order_sql_for_columns(columns)
    sql = f"""
    SELECT {select_sql}
    FROM `{table_ref}`
    WHERE match_id = @match_id
    ORDER BY {order_sql}
    LIMIT 1
    """
    rows = _safe_query(sql, [bigquery.ScalarQueryParameter("match_id", "INT64", match_id)])
    return rows[0] if rows else None


def _fetch_match_keys_rows(table_ref: str, match_id: int) -> List[Dict[str, Any]]:
    columns = _table_columns(table_ref)
    required = {"match_id"}
    if not required.issubset(columns):
        return []

    fields = [f for f in ["match_id", "match_key", "rank", "statement", "teams_json", "round_name"] if f in columns]
    if not fields:
        return []
    select_sql = ", ".join(fields)
    order_sql = "rank ASC" if "rank" in columns else _order_sql_for_columns(columns)
    sql = f"""
    SELECT {select_sql}
    FROM `{table_ref}`
    WHERE match_id = @match_id
    ORDER BY {order_sql}
    LIMIT 100
    """
    return _safe_query(sql, [bigquery.ScalarQueryParameter("match_id", "INT64", match_id)])


def _fetch_betting_stats_rows(table_ref: str, match_id: int) -> List[Dict[str, Any]]:
    columns = _table_columns(table_ref)
    if "match_id" not in columns:
        return []

    fields = [
        f
        for f in [
            "match_id",
            "category",
            "sub_tab",
            "label",
            "value",
            "home",
            "away",
            "total_matches_home",
            "total_matches_away",
        ]
        if f in columns
    ]
    if not fields:
        return []
    select_sql = ", ".join(fields)
    if "category" in columns:
        order_sql = "category ASC"
        if "sub_tab" in columns:
            order_sql += ", sub_tab ASC"
        if "label" in columns:
            order_sql += ", label ASC"
    elif "sub_tab" in columns:
        order_sql = "sub_tab ASC"
        if "label" in columns:
            order_sql += ", label ASC"
    elif "label" in columns:
        order_sql = "label ASC"
    else:
        order_sql = _order_sql_for_columns(columns)

    sql = f"""
    SELECT {select_sql}
    FROM `{table_ref}`
    WHERE match_id = @match_id
    ORDER BY {order_sql}
    LIMIT 300
    """
    return _safe_query(sql, [bigquery.ScalarQueryParameter("match_id", "INT64", match_id)])


def _fetch_last_matches_rows(table_ref: str, match_id: int) -> List[Dict[str, Any]]:
    columns = _table_columns(table_ref)
    if "match_id" not in columns:
        return []

    fields = [
        f
        for f in [
            "match_id",
            "side",
            "lm_match_id",
            "lm_date",
            "lm_ht",
            "lm_at",
            "lm_hscore",
            "lm_ascore",
            "lm_outcome",
            "lm_home",
            "lm_league_id",
            "lm_matchstatus",
            "lm_match_key",
            "lm_periods",
        ]
        if f in columns
    ]
    if not fields:
        return []
    select_sql = ", ".join(fields)
    order_sql = "SAFE_CAST(lm_date AS TIMESTAMP) DESC" if "lm_date" in columns else _order_sql_for_columns(columns)
    sql = f"""
    SELECT {select_sql}
    FROM `{table_ref}`
    WHERE match_id = @match_id
    ORDER BY {order_sql}
    LIMIT 100
    """
    return _safe_query(sql, [bigquery.ScalarQueryParameter("match_id", "INT64", match_id)])


def _league_key(league: str) -> Optional[str]:
    key = (league or "").strip().lower()
    return key if key in LEAGUE_TABLES else None


@router.get("/{league}/matchups/upcoming")
def soccer_matchups_upcoming(
    league: str,
    limit: int = Query(default=50, ge=1, le=500),
    lookahead_days: int = Query(default=14, ge=1, le=60),
):
    league_key = _league_key(league)
    if not league_key:
        return []

    tables = LEAGUE_TABLES[league_key]
    candidates: List[Tuple[str, str]] = [
        (tables["upcoming"], "start_time_utc"),
        (tables["match_info"], "date_utc"),
        (tables["match_info_fallback"], "date_utc"),
        (tables["match_keys"], "date_utc"),
    ]

    seen: set[str] = set()
    for table_ref, start_col in candidates:
        if not table_ref or table_ref in seen:
            continue
        seen.add(table_ref)
        rows = _query_upcoming_from_table(
            table_ref,
            start_time_column=start_col,
            lookahead_days=lookahead_days,
            limit=limit,
        )
        if rows:
            match_ids = [int(r["match_id"]) for r in rows if r.get("match_id") is not None]
            primary_form_map = _fetch_recent_form_map(tables["match_info"], match_ids)
            fallback_form_map = _fetch_recent_form_map(tables["match_info_fallback"], match_ids)

            for row in rows:
                match_id = row.get("match_id")
                if match_id is None:
                    row["home_recent_form"] = None
                    row["away_recent_form"] = None
                    row["odds_summary"] = None
                    continue
                current = primary_form_map.get(int(match_id)) or {}
                fallback = fallback_form_map.get(int(match_id)) or {}
                row["home_recent_form"] = current.get("home_recent_form") or fallback.get("home_recent_form")
                row["away_recent_form"] = current.get("away_recent_form") or fallback.get("away_recent_form")
            odds_summary_map = _fetch_odds_summary_for_matches(tables["odds"], match_ids)
            for row in rows:
                row_match_id = row.get("match_id")
                if row_match_id is None:
                    row["odds_summary"] = None
                    continue
                row["odds_summary"] = odds_summary_map.get(int(row_match_id))
            return rows

    return []


@router.get("/{league}/matchups/{match_id}")
def soccer_matchup_detail(league: str, match_id: int):
    league_key = _league_key(league)
    if not league_key:
        return {
            "league": league,
            "match_id": match_id,
            "match_info": None,
            "match_keys": [],
            "betting_stats": [],
            "last_matches": [],
            "odds_summary": None,
            "odds_board": [],
            "odds_updated_at": None,
        }

    tables = LEAGUE_TABLES[league_key]
    match_info = _fetch_match_info_row(tables["match_info"], match_id)
    if match_info is None:
        match_info = _fetch_match_info_row(tables["match_info_fallback"], match_id)
    odds_summary = _fetch_odds_summary_for_matches(tables["odds"], [match_id]).get(match_id)
    odds_board, odds_updated_at = _fetch_match_odds_board(tables["odds"], match_id)

    return {
        "league": league_key,
        "match_id": match_id,
        "match_info": match_info,
        "match_keys": _fetch_match_keys_rows(tables["match_keys"], match_id),
        "betting_stats": _fetch_betting_stats_rows(tables["betting_stats"], match_id),
        "last_matches": _fetch_last_matches_rows(tables["last_matches"], match_id),
        "odds_summary": odds_summary,
        "odds_board": odds_board,
        "odds_updated_at": odds_updated_at,
    }

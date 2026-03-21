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
    },
    "mls": {
        "upcoming": os.getenv("ODDSPEDIA_MLS_UPCOMING_MATCHES_TABLE", "oddspedia.mls_upcoming_matches"),
        "match_info": os.getenv("ODDSPEDIA_MLS_MATCH_INFO_TABLE", "oddspedia.mls_match_info"),
        "match_info_fallback": os.getenv("ODDSPEDIA_MLS_MATCH_WEATHER_TABLE", "oddspedia.mls_match_weather"),
        "match_keys": os.getenv("ODDSPEDIA_MLS_MATCH_KEYS_TABLE", "oddspedia.mls_match_keys"),
        "betting_stats": os.getenv("ODDSPEDIA_MLS_BETTING_STATS_TABLE", "oddspedia.mls_betting_stats"),
        "last_matches": os.getenv("ODDSPEDIA_MLS_LAST_MATCHES_TABLE", "oddspedia.mls_last_matches"),
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
        }

    tables = LEAGUE_TABLES[league_key]
    match_info = _fetch_match_info_row(tables["match_info"], match_id)
    if match_info is None:
        match_info = _fetch_match_info_row(tables["match_info_fallback"], match_id)

    return {
        "league": league_key,
        "match_id": match_id,
        "match_info": match_info,
        "match_keys": _fetch_match_keys_rows(tables["match_keys"], match_id),
        "betting_stats": _fetch_betting_stats_rows(tables["betting_stats"], match_id),
        "last_matches": _fetch_last_matches_rows(tables["last_matches"], match_id),
    }

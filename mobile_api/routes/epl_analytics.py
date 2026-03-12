from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, List
from urllib.parse import quote

from fastapi import APIRouter, Query
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from bq import get_bq_client
from ingest.epl.ingest import ingest_yesterday_refresh, run_full_ingestion

router = APIRouter(tags=["EPL"])

EPL_MATCHES_TABLE = os.getenv("EPL_MATCHES_TABLE", "epl_data.matches")
EPL_STANDINGS_TABLE = os.getenv("EPL_STANDINGS_TABLE", "epl_data.standings")
EPL_TEAMS_TABLE = os.getenv("EPL_TEAMS_TABLE", "epl_data.teams")
EPL_MATCH_EVENTS_TABLE = os.getenv("EPL_MATCH_EVENTS_TABLE", "epl_data.match_events")
EPL_TEAM_MASTER_METRICS_TABLE = os.getenv("EPL_TEAM_MASTER_METRICS_TABLE", "epl_data.team_master_metrics")

SOCCER_EPL_BETTING_ANALYTICS_TABLE = os.getenv(
    "SOCCER_EPL_BETTING_ANALYTICS_TABLE", "soccer_data.epl_betting_analytics"
)


def _split_table_ref(table_ref: str) -> tuple[str | None, str, str]:
    parts = table_ref.split('.')
    if len(parts) == 2:
        return None, parts[0], parts[1]
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    raise ValueError(f"Unsupported BigQuery table reference: {table_ref}")


def _table_columns(table_ref: str) -> set[str]:
    project, dataset, table = _split_table_ref(table_ref)
    client = get_bq_client()
    project = project or client.project
    sql = f"""
    SELECT column_name
    FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = @table
    """
    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("table", "STRING", table)]
        ),
    )
    return {r["column_name"] for r in job.result()}


def _select_or_null(column: str, columns: set[str]) -> str:
    return column if column in columns else f"NULL AS {column}"


def _logo_url(team_name: str) -> str:
    encoded = quote(team_name)
    return (
        "https://raw.githubusercontent.com/luukhopman/football-logos/master/"
        f"logos/England%20-%20Premier%20League/{encoded}.png"
    )


def _season_default() -> int:
    return datetime.now(timezone.utc).year


def _query(sql: str, params: List[bigquery.ScalarQueryParameter]) -> List[Dict[str, Any]]:
    client = get_bq_client()
    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(query_parameters=params),
    )
    return [dict(r) for r in job.result()]


@router.post("/ingest/epl/full")
def ingest_epl_full(current_season: int = Query(default_factory=_season_default)):
    return run_full_ingestion(current_season=current_season)


@router.post("/ingest/epl/yesterday-refresh")
def ingest_epl_yesterday(current_season: int = Query(default_factory=_season_default)):
    return ingest_yesterday_refresh(current_season=current_season)


@router.get("/epl/moneylines")
def epl_moneylines(current_season: int = Query(default_factory=_season_default), lookahead_days: int = 7):
    sql = f"""
    WITH latest_matches AS (
      SELECT payload
      FROM `{EPL_MATCHES_TABLE}`
      WHERE season = @season
      QUALIFY ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY ingested_at DESC) = 1
    ),
    latest_standings AS (
      SELECT payload
      FROM `{EPL_STANDINGS_TABLE}`
      WHERE season = @season
      QUALIFY ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(payload, '$.team.id') ORDER BY ingested_at DESC) = 1
    ),
    upcoming AS (
      SELECT
        CAST(JSON_VALUE(payload, '$.id') AS INT64) AS match_id,
        CAST(JSON_VALUE(payload, '$.home_team_id') AS INT64) AS home_team_id,
        CAST(JSON_VALUE(payload, '$.away_team_id') AS INT64) AS away_team_id,
        TIMESTAMP(JSON_VALUE(payload, '$.date')) AS match_time
      FROM latest_matches
      WHERE JSON_VALUE(payload, '$.status') NOT IN ('STATUS_FULL_TIME','STATUS_POSTPONED','STATUS_CANCELLED')
        AND DATE(TIMESTAMP(JSON_VALUE(payload, '$.date'))) BETWEEN CURRENT_DATE() AND DATE_ADD(CURRENT_DATE(), INTERVAL @lookahead_days DAY)
    ),
    team_power AS (
      SELECT
        CAST(JSON_VALUE(payload, '$.team.id') AS INT64) AS team_id,
        JSON_VALUE(payload, '$.team.name') AS team_name,
        CAST(JSON_VALUE(payload, '$.points') AS FLOAT64) / NULLIF(CAST(JSON_VALUE(payload, '$.games_played') AS FLOAT64), 0) AS ppg,
        CAST(JSON_VALUE(payload, '$.goals_for') AS FLOAT64) / NULLIF(CAST(JSON_VALUE(payload, '$.games_played') AS FLOAT64), 0) AS goals_for_pg,
        CAST(JSON_VALUE(payload, '$.goals_against') AS FLOAT64) / NULLIF(CAST(JSON_VALUE(payload, '$.games_played') AS FLOAT64), 0) AS goals_against_pg
      FROM latest_standings
    )
    SELECT
      u.match_id,
      u.match_time,
      hp.team_name AS home_team,
      ap.team_name AS away_team,
      ROUND(1 / NULLIF((1 / (1 + EXP(-((hp.ppg - ap.ppg) + 0.15)))), 0), 3) AS home_fair_decimal,
      ROUND(1 / NULLIF((1 / (1 + EXP(-((ap.ppg - hp.ppg) - 0.15)))), 0), 3) AS away_fair_decimal,
      ROUND(100 * (1 / (1 + EXP(-((hp.ppg - ap.ppg) + 0.15)))), 1) AS home_win_pct_model,
      ROUND(100 * (1 / (1 + EXP(-((ap.ppg - hp.ppg) - 0.15)))), 1) AS away_win_pct_model,
      ROUND(hp.goals_for_pg, 2) AS home_goals_for_pg,
      ROUND(hp.goals_against_pg, 2) AS home_goals_against_pg,
      ROUND(ap.goals_for_pg, 2) AS away_goals_for_pg,
      ROUND(ap.goals_against_pg, 2) AS away_goals_against_pg
    FROM upcoming u
    JOIN team_power hp ON u.home_team_id = hp.team_id
    JOIN team_power ap ON u.away_team_id = ap.team_id
    ORDER BY u.match_time
    """
    rows = _query(
        sql,
        [
            bigquery.ScalarQueryParameter("season", "INT64", current_season),
            bigquery.ScalarQueryParameter("lookahead_days", "INT64", lookahead_days),
        ],
    )
    for row in rows:
        row["home_logo"] = _logo_url(row.get("home_team") or "")
        row["away_logo"] = _logo_url(row.get("away_team") or "")
    return rows


@router.get("/epl/btts")
def epl_btts(current_season: int = Query(default_factory=_season_default), lookahead_days: int = 7):
    sql = f"""
    WITH latest_matches AS (
      SELECT payload
      FROM `{EPL_MATCHES_TABLE}`
      WHERE season = @season
      QUALIFY ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY ingested_at DESC) = 1
    ),
    completed AS (
      SELECT
        CAST(JSON_VALUE(payload, '$.home_team_id') AS INT64) AS home_team_id,
        CAST(JSON_VALUE(payload, '$.away_team_id') AS INT64) AS away_team_id,
        CAST(JSON_VALUE(payload, '$.home_score') AS INT64) AS home_score,
        CAST(JSON_VALUE(payload, '$.away_score') AS INT64) AS away_score
      FROM latest_matches
      WHERE JSON_VALUE(payload, '$.status') = 'STATUS_FULL_TIME'
    ),
    team_rates AS (
      SELECT team_id,
        AVG(scored) AS score_rate,
        AVG(conceded) AS concede_rate,
        AVG(CASE WHEN scored > 0 AND conceded > 0 THEN 1 ELSE 0 END) AS btts_rate
      FROM (
        SELECT home_team_id AS team_id, home_score AS scored, away_score AS conceded FROM completed
        UNION ALL
        SELECT away_team_id AS team_id, away_score AS scored, home_score AS conceded FROM completed
      )
      GROUP BY team_id
    ),
    teams AS (
      SELECT CAST(JSON_VALUE(payload, '$.id') AS INT64) AS team_id, JSON_VALUE(payload, '$.name') AS team_name
      FROM `{EPL_TEAMS_TABLE}`
      WHERE season = @season
      QUALIFY ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY ingested_at DESC) = 1
    ),
    upcoming AS (
      SELECT
        CAST(JSON_VALUE(payload, '$.id') AS INT64) AS match_id,
        CAST(JSON_VALUE(payload, '$.home_team_id') AS INT64) AS home_team_id,
        CAST(JSON_VALUE(payload, '$.away_team_id') AS INT64) AS away_team_id,
        TIMESTAMP(JSON_VALUE(payload, '$.date')) AS match_time
      FROM latest_matches
      WHERE JSON_VALUE(payload, '$.status') NOT IN ('STATUS_FULL_TIME','STATUS_POSTPONED','STATUS_CANCELLED')
        AND DATE(TIMESTAMP(JSON_VALUE(payload, '$.date'))) BETWEEN CURRENT_DATE() AND DATE_ADD(CURRENT_DATE(), INTERVAL @lookahead_days DAY)
    )
    SELECT
      u.match_id,
      u.match_time,
      ht.team_name AS home_team,
      at.team_name AS away_team,
      ROUND(100 * ((COALESCE(hr.score_rate,0.5) + COALESCE(ar.concede_rate,0.5))/2) * ((COALESCE(ar.score_rate,0.5) + COALESCE(hr.concede_rate,0.5))/2), 1) AS btts_yes_pct_model,
      ROUND(COALESCE(hr.btts_rate,0), 3) AS home_btts_hist,
      ROUND(COALESCE(ar.btts_rate,0), 3) AS away_btts_hist
    FROM upcoming u
    LEFT JOIN team_rates hr ON u.home_team_id = hr.team_id
    LEFT JOIN team_rates ar ON u.away_team_id = ar.team_id
    JOIN teams ht ON u.home_team_id = ht.team_id
    JOIN teams at ON u.away_team_id = at.team_id
    ORDER BY u.match_time
    """
    rows = _query(
        sql,
        [
            bigquery.ScalarQueryParameter("season", "INT64", current_season),
            bigquery.ScalarQueryParameter("lookahead_days", "INT64", lookahead_days),
        ],
    )
    for row in rows:
        row["home_logo"] = _logo_url(row.get("home_team") or "")
        row["away_logo"] = _logo_url(row.get("away_team") or "")
    return rows


@router.get("/epl/total-goals")
def epl_total_goals(current_season: int = Query(default_factory=_season_default), lookahead_days: int = 7):
    sql = f"""
    WITH latest_matches AS (
      SELECT payload
      FROM `{EPL_MATCHES_TABLE}`
      WHERE season = @season
      QUALIFY ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY ingested_at DESC) = 1
    ),
    completed AS (
      SELECT
        CAST(JSON_VALUE(payload, '$.home_team_id') AS INT64) AS home_team_id,
        CAST(JSON_VALUE(payload, '$.away_team_id') AS INT64) AS away_team_id,
        CAST(JSON_VALUE(payload, '$.home_score') AS FLOAT64) AS home_score,
        CAST(JSON_VALUE(payload, '$.away_score') AS FLOAT64) AS away_score
      FROM latest_matches
      WHERE JSON_VALUE(payload, '$.status') = 'STATUS_FULL_TIME'
    ),
    teams AS (
      SELECT CAST(JSON_VALUE(payload, '$.id') AS INT64) AS team_id, JSON_VALUE(payload, '$.name') AS team_name
      FROM `{EPL_TEAMS_TABLE}`
      WHERE season = @season
      QUALIFY ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY ingested_at DESC) = 1
    ),
    team_rates AS (
      SELECT
        team_id,
        AVG(goals_for) AS avg_goals_for,
        AVG(goals_against) AS avg_goals_against,
        AVG(total_goals) AS avg_total_goals,
        STDDEV(total_goals) AS total_goals_stddev,
        AVG(CASE WHEN total_goals >= 3 THEN 1 ELSE 0 END) AS over_2_5_rate
      FROM (
        SELECT home_team_id AS team_id, home_score AS goals_for, away_score AS goals_against, home_score + away_score AS total_goals FROM completed
        UNION ALL
        SELECT away_team_id AS team_id, away_score AS goals_for, home_score AS goals_against, home_score + away_score AS total_goals FROM completed
      )
      GROUP BY team_id
    ),
    upcoming AS (
      SELECT
        CAST(JSON_VALUE(payload, '$.id') AS INT64) AS match_id,
        CAST(JSON_VALUE(payload, '$.home_team_id') AS INT64) AS home_team_id,
        CAST(JSON_VALUE(payload, '$.away_team_id') AS INT64) AS away_team_id,
        TIMESTAMP(JSON_VALUE(payload, '$.date')) AS match_time
      FROM latest_matches
      WHERE JSON_VALUE(payload, '$.status') NOT IN ('STATUS_FULL_TIME','STATUS_POSTPONED','STATUS_CANCELLED')
        AND DATE(TIMESTAMP(JSON_VALUE(payload, '$.date'))) BETWEEN CURRENT_DATE() AND DATE_ADD(CURRENT_DATE(), INTERVAL @lookahead_days DAY)
    )
    SELECT
      u.match_id,
      u.match_time,
      ht.team_name AS home_team,
      at.team_name AS away_team,
      ROUND(hr.avg_goals_for, 2) AS home_avg_goals,
      ROUND(hr.avg_goals_against, 2) AS home_avg_goals_allowed,
      ROUND(ar.avg_goals_for, 2) AS away_avg_goals,
      ROUND(ar.avg_goals_against, 2) AS away_avg_goals_allowed,
      ROUND((hr.avg_total_goals + ar.avg_total_goals)/2, 2) AS opponent_total_avg_goals,
      ROUND(((hr.avg_goals_for + ar.avg_goals_against) + (ar.avg_goals_for + hr.avg_goals_against))/2, 2) AS projected_total_goals,
      ROUND((COALESCE(hr.over_2_5_rate,0) + COALESCE(ar.over_2_5_rate,0))/2, 3) AS over_2_5_rate_blend,
      ROUND((COALESCE(hr.total_goals_stddev,0) + COALESCE(ar.total_goals_stddev,0))/2, 2) AS total_goals_volatility
    FROM upcoming u
    LEFT JOIN team_rates hr ON u.home_team_id = hr.team_id
    LEFT JOIN team_rates ar ON u.away_team_id = ar.team_id
    JOIN teams ht ON u.home_team_id = ht.team_id
    JOIN teams at ON u.away_team_id = at.team_id
    ORDER BY u.match_time
    """
    rows = _query(
        sql,
        [
            bigquery.ScalarQueryParameter("season", "INT64", current_season),
            bigquery.ScalarQueryParameter("lookahead_days", "INT64", lookahead_days),
        ],
    )
    for row in rows:
        row["home_logo"] = _logo_url(row.get("home_team") or "")
        row["away_logo"] = _logo_url(row.get("away_team") or "")
    return rows


@router.get("/epl/cards")
def epl_cards(current_season: int = Query(default_factory=_season_default)):
    sql = f"""
    WITH latest_events AS (
      SELECT payload
      FROM `{EPL_MATCH_EVENTS_TABLE}`
      WHERE season = @season
      QUALIFY ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY ingested_at DESC) = 1
    ),
    teams AS (
      SELECT CAST(JSON_VALUE(payload, '$.id') AS INT64) AS team_id, JSON_VALUE(payload, '$.name') AS team_name
      FROM `{EPL_TEAMS_TABLE}`
      WHERE season = @season
      QUALIFY ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY ingested_at DESC) = 1
    ),
    card_events AS (
      SELECT
        CAST(JSON_VALUE(payload, '$.match_id') AS INT64) AS match_id,
        CAST(JSON_VALUE(payload, '$.team_id') AS INT64) AS team_id,
        JSON_VALUE(payload, '$.event_type') AS event_type
      FROM latest_events
      WHERE JSON_VALUE(payload, '$.event_type') IN ('yellow_card','red_card')
    ),
    team_cards AS (
      SELECT
        team_id,
        match_id,
        SUM(CASE WHEN event_type = 'yellow_card' THEN 1 ELSE 0 END) AS yellow_cards,
        SUM(CASE WHEN event_type = 'red_card' THEN 1 ELSE 0 END) AS red_cards
      FROM card_events
      GROUP BY team_id, match_id
    )
    SELECT
      t.team_name,
      COUNT(DISTINCT tc.match_id) AS matches_sample,
      ROUND(AVG(tc.yellow_cards), 2) AS avg_yellow_cards,
      ROUND(AVG(tc.red_cards), 2) AS avg_red_cards,
      ROUND(AVG(tc.yellow_cards + (2 * tc.red_cards)), 2) AS avg_card_points,
      ROUND(STDDEV(tc.yellow_cards + tc.red_cards), 2) AS card_volatility
    FROM team_cards tc
    JOIN teams t ON tc.team_id = t.team_id
    GROUP BY t.team_name
    ORDER BY avg_card_points DESC, avg_yellow_cards DESC
    """
    rows = _query(sql, [bigquery.ScalarQueryParameter("season", "INT64", current_season)])
    for row in rows:
        row["team_logo"] = _logo_url(row.get("team_name") or "")
    return rows


@router.get("/epl/upcoming-today")
def epl_upcoming_today(current_season: int = Query(default_factory=_season_default)):
    sql = f"""
    WITH latest_matches AS (
      SELECT payload
      FROM `{EPL_MATCHES_TABLE}`
      WHERE season = @season
      QUALIFY ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY ingested_at DESC) = 1
    ),
    teams AS (
      SELECT CAST(JSON_VALUE(payload, '$.id') AS INT64) AS team_id, JSON_VALUE(payload, '$.name') AS team_name
      FROM `{EPL_TEAMS_TABLE}`
      WHERE season = @season
      QUALIFY ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY ingested_at DESC) = 1
    )
    SELECT
      CAST(JSON_VALUE(m.payload, '$.id') AS INT64) AS match_id,
      JSON_VALUE(m.payload, '$.name') AS match_name,
      TIMESTAMP(JSON_VALUE(m.payload, '$.date')) AS match_time_utc,
      FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S %Z', TIMESTAMP(JSON_VALUE(m.payload, '$.date')), 'America/New_York') AS match_time_est,
      DATE(TIMESTAMP(JSON_VALUE(m.payload, '$.date')), 'America/New_York') AS match_date_est,
      CAST(JSON_VALUE(m.payload, '$.home_team_id') AS INT64) AS home_team_id,
      CAST(JSON_VALUE(m.payload, '$.away_team_id') AS INT64) AS away_team_id,
      ht.team_name AS home_team,
      at.team_name AS away_team,
      JSON_VALUE(m.payload, '$.status') AS status,
      JSON_VALUE(m.payload, '$.status_detail') AS status_detail,
      JSON_VALUE(m.payload, '$.venue_name') AS venue_name,
      JSON_VALUE(m.payload, '$.venue_city') AS venue_city
    FROM latest_matches m
    JOIN teams ht ON CAST(JSON_VALUE(m.payload, '$.home_team_id') AS INT64) = ht.team_id
    JOIN teams at ON CAST(JSON_VALUE(m.payload, '$.away_team_id') AS INT64) = at.team_id
    WHERE JSON_VALUE(m.payload, '$.status') NOT IN ('STATUS_FULL_TIME', 'STATUS_POSTPONED', 'STATUS_CANCELLED')
      AND DATE(TIMESTAMP(JSON_VALUE(m.payload, '$.date')), 'America/New_York') = CURRENT_DATE('America/New_York')
    ORDER BY match_time_utc
    """
    rows = _query(sql, [bigquery.ScalarQueryParameter("season", "INT64", current_season)])
    for row in rows:
        row["home_logo"] = _logo_url(row.get("home_team") or "")
        row["away_logo"] = _logo_url(row.get("away_team") or "")
    return rows


@router.get("/epl/betting-analytics")
def epl_betting_analytics(
    market: str | None = Query(default=None),
    bookmaker: str | None = Query(default=None),
    min_edge: float | None = Query(default=None),
    only_best_price: bool = Query(default=False),
    limit: int = Query(default=200, ge=1, le=1000),
):
    import logging, time, traceback
    log = logging.getLogger("epl_betting_analytics")
    log.setLevel(logging.DEBUG)
    t0 = time.time()
    log.info("[EPL-ANALYTICS] start table=%s market=%s bookmaker=%s min_edge=%s only_best_price=%s limit=%s",
             SOCCER_EPL_BETTING_ANALYTICS_TABLE, market, bookmaker, min_edge, only_best_price, limit)

    table_columns = _table_columns(SOCCER_EPL_BETTING_ANALYTICS_TABLE)

    filters = ["DATE(start_time_et, 'America/New_York') = CURRENT_DATE('America/New_York')"]
    params: List[bigquery.ScalarQueryParameter] = [
        bigquery.ScalarQueryParameter("limit", "INT64", limit)
    ]

    if market:
        filters.append("LOWER(market) = LOWER(@market)")
        params.append(bigquery.ScalarQueryParameter("market", "STRING", market))
    if bookmaker:
        filters.append("LOWER(bookmaker) = LOWER(@bookmaker)")
        params.append(bigquery.ScalarQueryParameter("bookmaker", "STRING", bookmaker))
    if min_edge is not None:
        if "probability_vs_market" in table_columns:
            filters.append("COALESCE(probability_vs_market, 0) >= @min_edge")
            params.append(bigquery.ScalarQueryParameter("min_edge", "FLOAT64", min_edge))
        else:
            log.warning("[EPL-ANALYTICS] probability_vs_market column missing; skipping min_edge filter")
    if only_best_price:
        if "is_best_price" in table_columns:
            filters.append("is_best_price = TRUE")
        else:
            log.warning("[EPL-ANALYTICS] is_best_price column missing; skipping only_best_price filter")

    where_sql = " AND ".join(filters)
    log.info("[EPL-ANALYTICS] WHERE: %s", where_sql)

    base_select = [
        "ingested_at",
        "league",
        "game",
        "start_time_et",
        _select_or_null("home_team", table_columns),
        _select_or_null("away_team", table_columns),
        "bookmaker",
        "market",
        "outcome",
        "line",
        "price",
        _select_or_null("implied_probability", table_columns),
        _select_or_null("no_vig_probability", table_columns),
        _select_or_null("market_hold", table_columns),
        _select_or_null("market_avg_price", table_columns),
        _select_or_null("market_min_price", table_columns),
        _select_or_null("market_max_price", table_columns),
        _select_or_null("market_consensus_fair_probability", table_columns),
        _select_or_null("probability_vs_market", table_columns),
        _select_or_null("is_best_price", table_columns),
        _select_or_null("price_rank", table_columns),
        _select_or_null("model_expected_total_goals", table_columns),
        _select_or_null("model_away_win_form_edge", table_columns),
        _select_or_null("model_home_win_form_edge", table_columns),
        _select_or_null("model_total_line_edge", table_columns),
        _select_or_null("model_edge_tier", table_columns),
        _select_or_null("analytics_updated_at", table_columns),
        # Rolling stats from soccer_data.epl_betting_analytics
        _select_or_null("home_l3_goals_pg", table_columns),
        _select_or_null("home_l5_goals_pg", table_columns),
        _select_or_null("home_l7_goals_pg", table_columns),
        _select_or_null("away_l3_goals_pg", table_columns),
        _select_or_null("away_l5_goals_pg", table_columns),
        _select_or_null("away_l7_goals_pg", table_columns),
        _select_or_null("home_l3_goals_allowed_pg", table_columns),
        _select_or_null("home_l5_goals_allowed_pg", table_columns),
        _select_or_null("home_l7_goals_allowed_pg", table_columns),
        _select_or_null("away_l3_goals_allowed_pg", table_columns),
        _select_or_null("away_l5_goals_allowed_pg", table_columns),
        _select_or_null("away_l7_goals_allowed_pg", table_columns),
        _select_or_null("home_l3_corners_pg", table_columns),
        _select_or_null("home_l5_corners_pg", table_columns),
        _select_or_null("home_l7_corners_pg", table_columns),
        _select_or_null("away_l3_corners_pg", table_columns),
        _select_or_null("away_l5_corners_pg", table_columns),
        _select_or_null("away_l7_corners_pg", table_columns),
        _select_or_null("home_l3_win_rate", table_columns),
        _select_or_null("home_l5_win_rate", table_columns),
        _select_or_null("home_l7_win_rate", table_columns),
        _select_or_null("away_l3_win_rate", table_columns),
        _select_or_null("away_l5_win_rate", table_columns),
        _select_or_null("away_l7_win_rate", table_columns),
    ]

    select_sql = ",\n        ".join(base_select)

    sql = f"""
    WITH base AS (
      SELECT
        {select_sql}
      FROM `{SOCCER_EPL_BETTING_ANALYTICS_TABLE}`
      WHERE {where_sql}
    ),
    markets AS (
      SELECT DISTINCT market
      FROM base
      WHERE market IS NOT NULL
    ),
    books AS (
      SELECT DISTINCT bookmaker
      FROM base
      WHERE bookmaker IS NOT NULL
    )
    SELECT AS STRUCT
      (SELECT ARRAY_AGG(market ORDER BY market) FROM markets) AS available_markets,
      (SELECT ARRAY_AGG(bookmaker ORDER BY bookmaker) FROM books) AS available_bookmakers,
      (SELECT COUNT(*) FROM base) AS row_count,
      ARRAY(
        SELECT AS STRUCT *
        FROM base
        ORDER BY start_time_et ASC, game, market, outcome, price DESC
        LIMIT @limit
      ) AS records
    """

    empty = {
        "date_et": datetime.now(timezone.utc).date().isoformat(),
        "row_count": 0,
        "available_markets": [],
        "available_bookmakers": [],
        "rows": [],
    }
    try:
        log.info("[EPL-ANALYTICS] firing BQ query")
        rows = _query(sql, params)
        log.info("[EPL-ANALYTICS] BQ query done in %.2fs, got %d top-level rows", time.time() - t0, len(rows))
    except NotFound as e:
        log.error("[EPL-ANALYTICS] NotFound: %s", e)
        return empty
    except Exception as e:
        log.error("[EPL-ANALYTICS] BQ error after %.2fs: %s\n%s", time.time() - t0, e, traceback.format_exc())
        raise

    if not rows:
        log.warning("[EPL-ANALYTICS] query returned 0 rows")
        return empty

    try:
        payload = dict(rows[0])
        # Nested ARRAY<STRUCT> rows are BigQuery Row objects; convert to plain dicts.
        nested = payload.get("records") or []
        log.info("[EPL-ANALYTICS] nested rows count=%d", len(nested))
        payload["rows"] = [dict(r) for r in nested]
        payload["date_et"] = datetime.now(timezone.utc).date().isoformat()
        log.info("[EPL-ANALYTICS] done total=%.2fs", time.time() - t0)
        return payload
    except Exception as e:
        log.error("[EPL-ANALYTICS] serialization error: %s\n%s", e, traceback.format_exc())
        raise


@router.get("/epl/standings")
def epl_standings():
    sql = f"""
    WITH latest_season AS (
      SELECT MAX(season) AS season
      FROM `{EPL_STANDINGS_TABLE}`
    ),
    latest_standings AS (
      SELECT payload
      FROM `{EPL_STANDINGS_TABLE}`
      WHERE season = (SELECT season FROM latest_season)
      QUALIFY ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(payload, '$.team.id') ORDER BY ingested_at DESC) = 1
    )
    SELECT
      CAST(JSON_VALUE(payload, '$.team.id') AS INT64) AS team_id,
      JSON_VALUE(payload, '$.team.name') AS team_name,
      JSON_VALUE(payload, '$.team.short_name') AS team_short_name,
      CAST(JSON_VALUE(payload, '$.rank') AS INT64) AS rank,
      CAST(JSON_VALUE(payload, '$.wins') AS INT64) AS wins,
      CAST(JSON_VALUE(payload, '$.losses') AS INT64) AS losses,
      CAST(JSON_VALUE(payload, '$.draws') AS INT64) AS draws,
      CAST(JSON_VALUE(payload, '$.points') AS INT64) AS points,
      CAST(JSON_VALUE(payload, '$.goal_difference') AS INT64) AS goal_differential,
      CONCAT(
        CAST(COALESCE(CAST(JSON_VALUE(payload, '$.wins') AS INT64), 0) AS STRING), '-',
        CAST(COALESCE(CAST(JSON_VALUE(payload, '$.losses') AS INT64), 0) AS STRING), '-',
        CAST(COALESCE(CAST(JSON_VALUE(payload, '$.draws') AS INT64), 0) AS STRING)
      ) AS win_loss_record,
      JSON_VALUE(payload, '$.description') AS standing_note
    FROM latest_standings
    ORDER BY rank ASC, points DESC, goal_differential DESC, team_name ASC
    """
    rows = _query(sql, [])
    for row in rows:
        row["team_logo"] = _logo_url(row.get("team_name") or "")
    return rows


@router.get("/epl/team-master-metrics")
def epl_team_master_metrics():
    sql = f"""
    SELECT
      team_id,
      team_name,
      team_short_name,
      rank,
      points,
      goal_differential,
      points_per_game,
      standing_note,
      season_avg_goals_scored,
      season_avg_goals_allowed,
      season_score_rate,
      season_allow_rate,
      last10_avg_scored,
      last10_avg_allowed,
      last5_avg_scored,
      last5_avg_allowed,
      last3_avg_scored,
      last3_avg_allowed,
      season_team_cards_pg,
      season_opponent_cards_pg,
      season_total_cards_pg,
      l10_team_cards_pg,
      l10_opponent_cards_pg,
      l10_total_cards_pg,
      l5_team_cards_pg,
      l5_opponent_cards_pg,
      l5_total_cards_pg,
      l3_team_cards_pg,
      l3_opponent_cards_pg,
      l3_total_cards_pg
    FROM `{EPL_TEAM_MASTER_METRICS_TABLE}`
    ORDER BY rank ASC, points DESC, team_name ASC
    """
    rows = _query(sql, [])
    for row in rows:
        row["team_logo"] = _logo_url(row.get("team_name") or "")
    return rows

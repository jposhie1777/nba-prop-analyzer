from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, List
from urllib.parse import quote

from fastapi import APIRouter, Query
from google.cloud import bigquery

from bq import get_bq_client
from ingest.epl.ingest import ingest_yesterday_refresh, run_full_ingestion

router = APIRouter(tags=["EPL"])

EPL_MATCHES_TABLE = os.getenv("EPL_MATCHES_TABLE", "epl_data.matches")
EPL_STANDINGS_TABLE = os.getenv("EPL_STANDINGS_TABLE", "epl_data.standings")
EPL_TEAMS_TABLE = os.getenv("EPL_TEAMS_TABLE", "epl_data.teams")
EPL_MATCH_EVENTS_TABLE = os.getenv("EPL_MATCH_EVENTS_TABLE", "epl_data.match_events")
EPL_TEAM_MASTER_METRICS_TABLE = os.getenv("EPL_TEAM_MASTER_METRICS_TABLE", "epl_data.team_master_metrics")


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


@router.get("/epl/standings")
def epl_standings():
    sql = f"""
    SELECT
      team_id,
      team_name,
      team_short_name,
      rank,
      wins,
      losses,
      draws,
      points,
      goal_differential,
      CONCAT(
        CAST(COALESCE(wins, 0) AS STRING), '-',
        CAST(COALESCE(losses, 0) AS STRING), '-',
        CAST(COALESCE(draws, 0) AS STRING)
      ) AS win_loss_record,
      standing_note
    FROM `{EPL_TEAM_MASTER_METRICS_TABLE}`
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

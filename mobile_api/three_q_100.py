from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional

from google.cloud import bigquery

from bq import get_bq_client

DATASET = "nba_goat_data"
TABLE_NAME = "three_q_100_tonight"

BASE_QUERY = """
WITH finished_games AS (
  SELECT
    game_id,
    game_date,
    home_team_abbr,
    away_team_abbr,
    home_score_q1,
    home_score_q2,
    home_score_q3,
    away_score_q1,
    away_score_q2,
    away_score_q3
  FROM `nba_goat_data.games`
  WHERE is_final = TRUE
    AND home_score_q1 IS NOT NULL
    AND home_score_q2 IS NOT NULL
    AND home_score_q3 IS NOT NULL
    AND away_score_q1 IS NOT NULL
    AND away_score_q2 IS NOT NULL
    AND away_score_q3 IS NOT NULL
),
team_rows AS (
  SELECT
    game_id,
    game_date,
    home_team_abbr AS team_abbr,
    away_team_abbr AS opponent_abbr,
    "HOME" AS side,
    home_score_q1 + home_score_q2 + home_score_q3 AS team_3q_points,
    away_score_q1 + away_score_q2 + away_score_q3 AS opp_3q_points
  FROM finished_games
  UNION ALL
  SELECT
    game_id,
    game_date,
    away_team_abbr AS team_abbr,
    home_team_abbr AS opponent_abbr,
    "AWAY" AS side,
    away_score_q1 + away_score_q2 + away_score_q3 AS team_3q_points,
    home_score_q1 + home_score_q2 + home_score_q3 AS opp_3q_points
  FROM finished_games
),
team_stats AS (
  SELECT
    team_abbr,
    COUNT(*) AS games_played,
    AVG(team_3q_points) AS avg_3q_points,
    AVG(CASE WHEN team_3q_points >= 100 THEN 1 ELSE 0 END) AS hit_100_rate
  FROM team_rows
  GROUP BY team_abbr
),
opponent_stats AS (
  SELECT
    opponent_abbr AS team_abbr,
    COUNT(*) AS games_defended,
    AVG(opp_3q_points) AS avg_3q_allowed,
    AVG(CASE WHEN opp_3q_points >= 100 THEN 1 ELSE 0 END) AS allow_100_rate
  FROM team_rows
  GROUP BY opponent_abbr
),
today_games AS (
  SELECT
    game_id,
    game_date,
    start_time_est,
    home_team_abbr,
    away_team_abbr
  FROM `nba_goat_data.games`
  WHERE game_date = @game_date
),
matchups AS (
  SELECT
    game_id,
    game_date,
    start_time_est,
    home_team_abbr,
    away_team_abbr,
    home_team_abbr AS team_abbr,
    away_team_abbr AS opponent_abbr,
    "HOME" AS side
  FROM today_games
  UNION ALL
  SELECT
    game_id,
    game_date,
    start_time_est,
    home_team_abbr,
    away_team_abbr,
    away_team_abbr AS team_abbr,
    home_team_abbr AS opponent_abbr,
    "AWAY" AS side
  FROM today_games
)
SELECT
  @game_date AS run_date,
  CURRENT_TIMESTAMP() AS generated_at,
  matchups.game_id,
  matchups.game_date,
  matchups.start_time_est,
  matchups.home_team_abbr,
  matchups.away_team_abbr,
  matchups.team_abbr,
  matchups.opponent_abbr,
  matchups.side,
  team_stats.games_played,
  opponent_stats.games_defended,
  team_stats.avg_3q_points,
  opponent_stats.avg_3q_allowed,
  team_stats.hit_100_rate,
  opponent_stats.allow_100_rate,
  CASE
    WHEN team_stats.hit_100_rate IS NULL
      OR opponent_stats.allow_100_rate IS NULL
      THEN NULL
    ELSE (team_stats.hit_100_rate + opponent_stats.allow_100_rate) / 2
  END AS predicted_hit_rate,
  CASE
    WHEN team_stats.avg_3q_points IS NULL
      OR opponent_stats.avg_3q_allowed IS NULL
      THEN NULL
    ELSE (team_stats.avg_3q_points + opponent_stats.avg_3q_allowed) / 2
  END AS predicted_3q_points
FROM matchups
LEFT JOIN team_stats
  ON matchups.team_abbr = team_stats.team_abbr
LEFT JOIN opponent_stats
  ON matchups.opponent_abbr = opponent_stats.team_abbr
ORDER BY matchups.start_time_est, matchups.side
"""


def _normalize_game_date(game_date: date | str) -> date:
    if isinstance(game_date, date):
        return game_date
    return date.fromisoformat(game_date)


def _table_id(client: bigquery.Client) -> str:
    return f"{client.project}.{DATASET}.{TABLE_NAME}"


def query_three_q_100_rows(
    client: bigquery.Client,
    game_date: date | str,
) -> List[Dict[str, Any]]:
    query_date = _normalize_game_date(game_date)
    job = client.query(
        BASE_QUERY,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("game_date", "DATE", query_date)
            ]
        ),
    )
    return [dict(row) for row in job.result()]


def fetch_three_q_100_rows(
    client: bigquery.Client,
    game_date: date | str,
) -> List[Dict[str, Any]]:
    query_date = _normalize_game_date(game_date)
    table_id = _table_id(client)
    query = f"""
    SELECT *
    FROM `{table_id}`
    WHERE game_date = @game_date
    ORDER BY start_time_est, side
    """
    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("game_date", "DATE", query_date)
            ]
        ),
    )
    return [dict(row) for row in job.result()]


def refresh_three_q_100_predictions(
    game_date: date | str,
    client: Optional[bigquery.Client] = None,
) -> Dict[str, Any]:
    bq_client = client or get_bq_client()
    query_date = _normalize_game_date(game_date)
    table_id = _table_id(bq_client)
    query = f"""
    CREATE OR REPLACE TABLE `{table_id}` AS
    {BASE_QUERY}
    """
    job = bq_client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("game_date", "DATE", query_date)
            ]
        ),
    )
    job.result()
    table = bq_client.get_table(table_id)
    return {
        "table": table_id,
        "game_date": query_date.isoformat(),
        "rows": table.num_rows,
    }

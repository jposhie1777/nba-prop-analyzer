# routes/season_averages.py
"""
API routes for Season Averages data (mobile app consumption)
"""

from fastapi import APIRouter, Query
from typing import Optional
from bq import get_bq_client

router = APIRouter(prefix="/season-averages", tags=["Season Averages"])


# ======================================================
# PLAYER SEASON AVERAGES
# ======================================================

@router.get("/players")
def get_player_season_averages(
    season: int = Query(2025, description="Season year"),
    season_type: str = Query("regular", description="regular, playoffs, ist, playin"),
    search: Optional[str] = Query(None, description="Search by player name"),
    limit: int = Query(500, ge=1, le=1000),
):
    """
    Get player season averages (general/base stats).
    Returns data from the latest ingest run.
    """
    client = get_bq_client()

    # Build WHERE clause
    where_clauses = [
        f"season = {season}",
        f"season_type = '{season_type}'",
        "category = 'general'",
        "stat_type = 'base'",
    ]

    if search:
        # Escape single quotes in search term
        safe_search = search.replace("'", "''")
        where_clauses.append(
            f"(LOWER(player_first_name) LIKE LOWER('%{safe_search}%') "
            f"OR LOWER(player_last_name) LIKE LOWER('%{safe_search}%'))"
        )

    where_sql = " AND ".join(where_clauses)

    query = f"""
    WITH latest AS (
        SELECT MAX(run_ts) as max_run_ts
        FROM nba_live.player_season_averages
        WHERE season = {season}
          AND season_type = '{season_type}'
          AND category = 'general'
          AND stat_type = 'base'
    )
    SELECT
        player_id,
        player_first_name,
        player_last_name,
        player_position,
        player_height,
        player_weight,
        player_jersey_number,
        player_college,
        player_country,
        player_draft_year,
        player_draft_round,
        player_draft_number,
        season,
        season_type,
        -- Core stats from JSON
        CAST(JSON_VALUE(stats, '$.gp') AS INT64) AS gp,
        CAST(JSON_VALUE(stats, '$.w') AS INT64) AS w,
        CAST(JSON_VALUE(stats, '$.l') AS INT64) AS l,
        CAST(JSON_VALUE(stats, '$.min') AS FLOAT64) AS min,
        CAST(JSON_VALUE(stats, '$.pts') AS FLOAT64) AS pts,
        CAST(JSON_VALUE(stats, '$.reb') AS FLOAT64) AS reb,
        CAST(JSON_VALUE(stats, '$.ast') AS FLOAT64) AS ast,
        CAST(JSON_VALUE(stats, '$.stl') AS FLOAT64) AS stl,
        CAST(JSON_VALUE(stats, '$.blk') AS FLOAT64) AS blk,
        CAST(JSON_VALUE(stats, '$.tov') AS FLOAT64) AS tov,
        CAST(JSON_VALUE(stats, '$.pf') AS FLOAT64) AS pf,
        CAST(JSON_VALUE(stats, '$.oreb') AS FLOAT64) AS oreb,
        CAST(JSON_VALUE(stats, '$.dreb') AS FLOAT64) AS dreb,
        CAST(JSON_VALUE(stats, '$.fga') AS FLOAT64) AS fga,
        CAST(JSON_VALUE(stats, '$.fgm') AS FLOAT64) AS fgm,
        CAST(JSON_VALUE(stats, '$.fg_pct') AS FLOAT64) AS fg_pct,
        CAST(JSON_VALUE(stats, '$.fg3a') AS FLOAT64) AS fg3a,
        CAST(JSON_VALUE(stats, '$.fg3m') AS FLOAT64) AS fg3m,
        CAST(JSON_VALUE(stats, '$.fg3_pct') AS FLOAT64) AS fg3_pct,
        CAST(JSON_VALUE(stats, '$.fta') AS FLOAT64) AS fta,
        CAST(JSON_VALUE(stats, '$.ftm') AS FLOAT64) AS ftm,
        CAST(JSON_VALUE(stats, '$.ft_pct') AS FLOAT64) AS ft_pct,
        CAST(JSON_VALUE(stats, '$.dd2') AS INT64) AS dd2,
        CAST(JSON_VALUE(stats, '$.td3') AS INT64) AS td3,
        CAST(JSON_VALUE(stats, '$.plus_minus') AS FLOAT64) AS plus_minus,
        CAST(JSON_VALUE(stats, '$.nba_fantasy_pts') AS FLOAT64) AS nba_fantasy_pts,
        CAST(JSON_VALUE(stats, '$.w_pct') AS FLOAT64) AS w_pct,
        -- Ranks
        CAST(JSON_VALUE(stats, '$.pts_rank') AS INT64) AS pts_rank,
        CAST(JSON_VALUE(stats, '$.reb_rank') AS INT64) AS reb_rank,
        CAST(JSON_VALUE(stats, '$.ast_rank') AS INT64) AS ast_rank,
        CAST(JSON_VALUE(stats, '$.stl_rank') AS INT64) AS stl_rank,
        CAST(JSON_VALUE(stats, '$.blk_rank') AS INT64) AS blk_rank,
    FROM nba_live.player_season_averages, latest
    WHERE {where_sql}
      AND run_ts = latest.max_run_ts
    ORDER BY pts DESC
    LIMIT {limit}
    """

    rows = [dict(r) for r in client.query(query).result()]

    return {
        "rows": rows,
        "count": len(rows),
        "season": season,
        "season_type": season_type,
    }


@router.get("/players/schema")
def get_player_season_averages_schema():
    """Get column schema for auto-table support."""
    return [
        {"name": "player_first_name", "type": "STRING"},
        {"name": "player_last_name", "type": "STRING"},
        {"name": "player_position", "type": "STRING"},
        {"name": "gp", "type": "INTEGER"},
        {"name": "min", "type": "FLOAT"},
        {"name": "pts", "type": "FLOAT"},
        {"name": "reb", "type": "FLOAT"},
        {"name": "ast", "type": "FLOAT"},
        {"name": "stl", "type": "FLOAT"},
        {"name": "blk", "type": "FLOAT"},
        {"name": "tov", "type": "FLOAT"},
        {"name": "fg_pct", "type": "FLOAT"},
        {"name": "fg3_pct", "type": "FLOAT"},
        {"name": "ft_pct", "type": "FLOAT"},
        {"name": "plus_minus", "type": "FLOAT"},
        {"name": "nba_fantasy_pts", "type": "FLOAT"},
    ]


# ======================================================
# TEAM SEASON AVERAGES
# ======================================================

@router.get("/teams")
def get_team_season_averages(
    season: int = Query(2025, description="Season year"),
    season_type: str = Query("regular", description="regular, playoffs, ist, playin"),
    search: Optional[str] = Query(None, description="Search by team name"),
    limit: int = Query(30, ge=1, le=100),
):
    """
    Get team season averages (general/base stats).
    Returns data from the latest ingest run.
    """
    client = get_bq_client()

    # Build WHERE clause
    where_clauses = [
        f"season = {season}",
        f"season_type = '{season_type}'",
        "category = 'general'",
        "stat_type = 'base'",
    ]

    if search:
        safe_search = search.replace("'", "''")
        where_clauses.append(
            f"(LOWER(team_name) LIKE LOWER('%{safe_search}%') "
            f"OR LOWER(team_full_name) LIKE LOWER('%{safe_search}%') "
            f"OR LOWER(team_abbreviation) LIKE LOWER('%{safe_search}%'))"
        )

    where_sql = " AND ".join(where_clauses)

    query = f"""
    WITH latest AS (
        SELECT MAX(run_ts) as max_run_ts
        FROM nba_live.team_season_averages
        WHERE season = {season}
          AND season_type = '{season_type}'
          AND category = 'general'
          AND stat_type = 'base'
    )
    SELECT
        team_id,
        team_conference,
        team_division,
        team_city,
        team_name,
        team_full_name,
        team_abbreviation,
        season,
        season_type,
        -- Core stats from JSON
        CAST(JSON_VALUE(stats, '$.gp') AS INT64) AS gp,
        CAST(JSON_VALUE(stats, '$.w') AS INT64) AS w,
        CAST(JSON_VALUE(stats, '$.l') AS INT64) AS l,
        CAST(JSON_VALUE(stats, '$.min') AS FLOAT64) AS min,
        CAST(JSON_VALUE(stats, '$.pts') AS FLOAT64) AS pts,
        CAST(JSON_VALUE(stats, '$.reb') AS FLOAT64) AS reb,
        CAST(JSON_VALUE(stats, '$.ast') AS FLOAT64) AS ast,
        CAST(JSON_VALUE(stats, '$.stl') AS FLOAT64) AS stl,
        CAST(JSON_VALUE(stats, '$.blk') AS FLOAT64) AS blk,
        CAST(JSON_VALUE(stats, '$.tov') AS FLOAT64) AS tov,
        CAST(JSON_VALUE(stats, '$.pf') AS FLOAT64) AS pf,
        CAST(JSON_VALUE(stats, '$.oreb') AS FLOAT64) AS oreb,
        CAST(JSON_VALUE(stats, '$.dreb') AS FLOAT64) AS dreb,
        CAST(JSON_VALUE(stats, '$.fga') AS FLOAT64) AS fga,
        CAST(JSON_VALUE(stats, '$.fgm') AS FLOAT64) AS fgm,
        CAST(JSON_VALUE(stats, '$.fg_pct') AS FLOAT64) AS fg_pct,
        CAST(JSON_VALUE(stats, '$.fg3a') AS FLOAT64) AS fg3a,
        CAST(JSON_VALUE(stats, '$.fg3m') AS FLOAT64) AS fg3m,
        CAST(JSON_VALUE(stats, '$.fg3_pct') AS FLOAT64) AS fg3_pct,
        CAST(JSON_VALUE(stats, '$.fta') AS FLOAT64) AS fta,
        CAST(JSON_VALUE(stats, '$.ftm') AS FLOAT64) AS ftm,
        CAST(JSON_VALUE(stats, '$.ft_pct') AS FLOAT64) AS ft_pct,
        CAST(JSON_VALUE(stats, '$.w_pct') AS FLOAT64) AS w_pct,
        CAST(JSON_VALUE(stats, '$.plus_minus') AS FLOAT64) AS plus_minus,
        -- Ranks
        CAST(JSON_VALUE(stats, '$.pts_rank') AS INT64) AS pts_rank,
        CAST(JSON_VALUE(stats, '$.reb_rank') AS INT64) AS reb_rank,
        CAST(JSON_VALUE(stats, '$.ast_rank') AS INT64) AS ast_rank,
        CAST(JSON_VALUE(stats, '$.w_rank') AS INT64) AS w_rank,
        CAST(JSON_VALUE(stats, '$.l_rank') AS INT64) AS l_rank,
    FROM nba_live.team_season_averages, latest
    WHERE {where_sql}
      AND run_ts = latest.max_run_ts
    ORDER BY w DESC, pts DESC
    LIMIT {limit}
    """

    rows = [dict(r) for r in client.query(query).result()]

    return {
        "rows": rows,
        "count": len(rows),
        "season": season,
        "season_type": season_type,
    }


@router.get("/teams/schema")
def get_team_season_averages_schema():
    """Get column schema for auto-table support."""
    return [
        {"name": "team_abbreviation", "type": "STRING"},
        {"name": "team_full_name", "type": "STRING"},
        {"name": "team_conference", "type": "STRING"},
        {"name": "w", "type": "INTEGER"},
        {"name": "l", "type": "INTEGER"},
        {"name": "w_pct", "type": "FLOAT"},
        {"name": "pts", "type": "FLOAT"},
        {"name": "reb", "type": "FLOAT"},
        {"name": "ast", "type": "FLOAT"},
        {"name": "stl", "type": "FLOAT"},
        {"name": "blk", "type": "FLOAT"},
        {"name": "tov", "type": "FLOAT"},
        {"name": "fg_pct", "type": "FLOAT"},
        {"name": "fg3_pct", "type": "FLOAT"},
        {"name": "ft_pct", "type": "FLOAT"},
        {"name": "plus_minus", "type": "FLOAT"},
    ]

# routes/bad_lines.py
from fastapi import APIRouter
from google.cloud import bigquery
from bq import get_bq_client

router = APIRouter(prefix="/bad-lines", tags=["bad-lines"])


@router.get("")
def get_bad_lines(
    min_score: float = 1.25,
    limit: int = 50,
):
    """Pre-live bad lines (before game start)"""
    bq = get_bq_client()

    rows = list(
        bq.query(
            """
            SELECT
              prop_id,
              game_id,
              player_id,
              player_name,

              home_team_abbr,
              away_team_abbr,
              opponent_team_abbr,

              market,
              market_window,
              odds_side,

              line_value,
              odds,

              hit_rate_l5,
              hit_rate_l10,
              hit_rate_l20,

              baseline_l10,
              expected_stat,
              expected_edge,

              opp_allowed_rank,
              defense_multiplier,

              bad_line_score
            FROM nba_live.v_bad_line_alerts_scored
            WHERE is_bad_line = TRUE
              AND bad_line_score >= @min_score
            ORDER BY bad_line_score DESC
            LIMIT @limit
            """,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(
                        "min_score", "FLOAT64", min_score
                    ),
                    bigquery.ScalarQueryParameter(
                        "limit", "INT64", limit
                    ),
                ]
            ),
        )
    )

    return {
        "count": len(rows),
        "bad_lines": rows,
    }


@router.get("/live")
def get_live_bad_lines(
    min_edge: float = 0.15,
    limit: int = 50,
):
    """
    Live bad lines during active games.

    Uses live player stats to calculate on-pace projections
    and compares to current lines to find edges.
    """
    bq = get_bq_client()

    rows = list(
        bq.query(
            """
            WITH live_props AS (
              SELECT
                o.game_id,
                o.player_id,
                o.market,
                o.line,
                o.book,
                o.over_odds,
                o.under_odds,
                o.snapshot_ts,
                g.state AS game_state,
                g.home_team_abbr,
                g.away_team_abbr,
                g.clock,
                g.period,
                ps.pts,
                ps.reb,
                ps.ast,
                ps.fg3_made,
                ps.minutes,
                p.player_name
              FROM `nba_live.v_live_player_prop_odds_latest` o
              JOIN `nba_live.live_games` g
                ON o.game_id = g.game_id
              LEFT JOIN (
                SELECT *, ROW_NUMBER() OVER (
                  PARTITION BY game_id, player_id
                  ORDER BY ingested_at DESC
                ) AS rn
                FROM `nba_live.live_player_stats`
              ) ps
                ON o.game_id = ps.game_id
                AND o.player_id = ps.player_id
                AND ps.rn = 1
              LEFT JOIN `nba_goat_data.player_lookup` p
                ON o.player_id = p.player_id
              WHERE g.state = 'LIVE'
            ),

            with_pace AS (
              SELECT
                *,
                -- On-pace calculation (stat / minutes * 48)
                CASE market
                  WHEN 'pts' THEN SAFE_DIVIDE(pts, NULLIF(minutes, 0)) * 48
                  WHEN 'reb' THEN SAFE_DIVIDE(reb, NULLIF(minutes, 0)) * 48
                  WHEN 'ast' THEN SAFE_DIVIDE(ast, NULLIF(minutes, 0)) * 48
                  WHEN '3pm' THEN SAFE_DIVIDE(fg3_made, NULLIF(minutes, 0)) * 48
                  WHEN 'pra' THEN SAFE_DIVIDE(pts + reb + ast, NULLIF(minutes, 0)) * 48
                  WHEN 'pr' THEN SAFE_DIVIDE(pts + reb, NULLIF(minutes, 0)) * 48
                  WHEN 'pa' THEN SAFE_DIVIDE(pts + ast, NULLIF(minutes, 0)) * 48
                  WHEN 'ra' THEN SAFE_DIVIDE(reb + ast, NULLIF(minutes, 0)) * 48
                  ELSE NULL
                END AS on_pace_stat,

                -- Current stat
                CASE market
                  WHEN 'pts' THEN pts
                  WHEN 'reb' THEN reb
                  WHEN 'ast' THEN ast
                  WHEN '3pm' THEN fg3_made
                  WHEN 'pra' THEN pts + reb + ast
                  WHEN 'pr' THEN pts + reb
                  WHEN 'pa' THEN pts + ast
                  WHEN 'ra' THEN reb + ast
                  ELSE NULL
                END AS current_stat
              FROM live_props
              WHERE minutes > 3  -- Only players with meaningful minutes
            ),

            with_edge AS (
              SELECT
                *,
                -- Edge = (on_pace - line) / line
                SAFE_DIVIDE(on_pace_stat - line, NULLIF(line, 0)) AS edge,
                -- Remaining to line
                line - current_stat AS remaining_to_line
              FROM with_pace
              WHERE on_pace_stat IS NOT NULL
            )

            SELECT
              game_id,
              player_id,
              player_name,
              home_team_abbr,
              away_team_abbr,
              market,
              'live' AS market_window,
              'over' AS odds_side,
              line AS line_value,
              over_odds AS odds,
              current_stat,
              on_pace_stat AS expected_stat,
              edge AS expected_edge,
              remaining_to_line,
              minutes AS live_minutes,
              period,
              clock,
              game_state,
              -- Score based on edge magnitude
              ABS(edge) * 10 AS bad_line_score
            FROM with_edge
            WHERE edge >= @min_edge  -- Positive edge = on pace to beat the line
            ORDER BY edge DESC
            LIMIT @limit
            """,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(
                        "min_edge", "FLOAT64", min_edge
                    ),
                    bigquery.ScalarQueryParameter(
                        "limit", "INT64", limit
                    ),
                ]
            ),
        )
    )

    return {
        "count": len(rows),
        "bad_lines": [dict(row) for row in rows],
    }

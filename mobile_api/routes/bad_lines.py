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

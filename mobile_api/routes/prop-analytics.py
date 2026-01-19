from fastapi import APIRouter
from google.cloud import bigquery
from pydantic import BaseModel
from typing import Optional

from bq import get_bq_client

router = APIRouter(prefix="/live", tags=["Live"])


class LivePropAnalyticsResponse(BaseModel):
    fair_odds: Optional[int] = None
    implied_prob: Optional[float] = None
    on_pace_value: Optional[float] = None
    delta_vs_pace: Optional[float] = None
    hit_rate_l5: Optional[float] = None
    hit_rate_l10: Optional[float] = None
    blowout_flag: Optional[bool] = None


@router.get(
    "/prop-analytics",
    response_model=LivePropAnalyticsResponse,
)
def get_live_prop_analytics(
    game_id: int,
    player_id: int,
    market: str,
    line: float,
    side: str,
):
    market = market.lower()
    side = side.lower()

    if side not in {"over", "under", "milestone"}:
        return {}

    client = get_bq_client()

    query = """
    SELECT
      fair_odds,
      implied_prob,
      on_pace_value,
      delta_vs_pace,
      hit_rate_l5,
      hit_rate_l10,
      blowout_flag
    FROM nba_live.v_live_prop_analytics
    WHERE game_id = @game_id
      AND player_id = @player_id
      AND market = @market
      AND line = @line
      AND side = @side
    LIMIT 1
    """

    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("game_id", "INT64", game_id),
                bigquery.ScalarQueryParameter("player_id", "INT64", player_id),
                bigquery.ScalarQueryParameter("market", "STRING", market),
                bigquery.ScalarQueryParameter("line", "FLOAT64", line),
                bigquery.ScalarQueryParameter("side", "STRING", side),
            ]
        ),
    )

    rows = list(job.result())
    return dict(rows[0]) if rows else {}

# mobile_api/routes/props.py

from fastapi import APIRouter, Query
from typing import Optional
from google.cloud import bigquery

from bq import get_bq_client

router = APIRouter(
    prefix="/props",
    tags=["props"],
)

DATASET = "nba_live"
VIEW = "v_player_prop_odds_master"


def build_where(
    game_date: Optional[str],
    market: Optional[str],
    window: Optional[str],
    min_confidence: Optional[float],
):
    clauses = []

    if game_date:
        clauses.append("game_date = @game_date")

    if market:
        clauses.append("market_key = @market")

    if window:
        clauses.append("market_window = @window")

    if min_confidence is not None:
        clauses.append("confidence >= @min_confidence")

    if not clauses:
        return ""

    return "WHERE " + " AND ".join(clauses)


@router.get("")
def read_props(
    game_date: Optional[str] = None,
    market: Optional[str] = None,
    window: Optional[str] = None,
    min_confidence: Optional[float] = None,
    limit: int = Query(200, le=500),
    offset: int = 0,
):
    client = get_bq_client()

    where_sql = build_where(
        game_date,
        market,
        window,
        min_confidence,
    )

    sql = f"""
    SELECT *
    FROM `{DATASET}.{VIEW}`
    {where_sql}
    ORDER BY hit_rate_over_l10 DESC
    LIMIT @limit
    OFFSET @offset
    """

    params = [
        bigquery.ScalarQueryParameter("limit", "INT64", limit),
        bigquery.ScalarQueryParameter("offset", "INT64", offset),
    ]

    if game_date:
        params.append(bigquery.ScalarQueryParameter("game_date", "STRING", game_date))
    if market:
        params.append(bigquery.ScalarQueryParameter("market", "STRING", market))
    if window:
        params.append(bigquery.ScalarQueryParameter("window", "STRING", window))
    if min_confidence is not None:
        params.append(bigquery.ScalarQueryParameter("min_confidence", "FLOAT64", min_confidence))

    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(query_parameters=params),
    )

    rows = [dict(r) for r in job.result()]

    return {
        "count": len(rows),
        "limit": limit,
        "offset": offset,
        "props": rows,
    }
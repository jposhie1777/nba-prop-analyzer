# mobile_api/routes/props.py

from fastapi import APIRouter, Query
from typing import Optional
from google.cloud import bigquery

from bq import get_bq_client

router = APIRouter(
    prefix="/props",
    tags=["props"],
)

DATASET = "odds_raw"
VIEW = "v_nba_alt_player_props"


def build_where(
    game_date: Optional[str],
    market: Optional[str],
):
    clauses = []

    if game_date:
        clauses.append("request_date = @game_date")

    if market:
        clauses.append("market_key = @market")

    if not clauses:
        return ""

    return "WHERE " + " AND ".join(clauses)


@router.get("")
def read_props(
    game_date: Optional[str] = None,
    market: Optional[str] = None,
    window: Optional[str] = None,
    limit: int = Query(500, ge=100, le=2000),
    offset: int = 0,
):
    client = get_bq_client()

    where_sql = build_where(
        game_date,
        market,
    )

    sql = f"""
    SELECT *
    FROM `{DATASET}.{VIEW}`
    {where_sql}
    ORDER BY commence_time ASC, event_id, player_name, market_key, line
    LIMIT @limit
    OFFSET @offset
    """

    params = [
        bigquery.ScalarQueryParameter("limit", "INT64", limit),
        bigquery.ScalarQueryParameter("offset", "INT64", offset),
    ]

    if game_date:
        params.append(
            bigquery.ScalarQueryParameter("game_date", "STRING", game_date)
        )
    if market:
        params.append(
            bigquery.ScalarQueryParameter("market", "STRING", market)
        )

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
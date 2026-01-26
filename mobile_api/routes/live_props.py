from fastapi import APIRouter, Query
from google.cloud import bigquery
from typing import Optional

from bq import get_bq_client

router = APIRouter(
    prefix="/live-props",
    tags=["live-props"],
)

VIEW = "nba_live.v_live_player_props_enriched"


@router.get("")
def read_live_props(
    limit: int = Query(100, ge=10, le=500),
    market: Optional[str] = None,
    book: Optional[str] = None,
):
    client = get_bq_client()

    where = []
    params = []

    if market:
        where.append("market = @market")
        params.append(
            bigquery.ScalarQueryParameter("market", "STRING", market)
        )

    if book:
        where.append("book = @book")
        params.append(
            bigquery.ScalarQueryParameter("book", "STRING", book)
        )

    where_sql = f"WHERE {' AND '.join(where)}" if where else ""

    query = f"""
        SELECT *
        FROM `{VIEW}`
        {where_sql}
        ORDER BY snapshot_ts DESC
        LIMIT @limit
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
            *params,
        ]
    )

    rows = client.query(query, job_config=job_config).result()

    return {
        "props": [dict(row) for row in rows]
    }
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
    limit: int = Query(100, ge=10, le=200),
    cursor: Optional[str] = None,          # 👈 NEW
    market: Optional[str] = None,
    book: Optional[str] = None,
):
    client = get_bq_client()

    where = []
    params = [
        bigquery.ScalarQueryParameter("limit", "INT64", limit),
    ]

    # ---- Cursor pagination ----
    if cursor:
        where.append("snapshot_ts < @cursor")
        params.append(
            bigquery.ScalarQueryParameter(
                "cursor", "TIMESTAMP", cursor
            )
        )

    # ---- Optional filters ----
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

    rows = list(
        client.query(
            query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=params
            ),
        ).result()
    )

    next_cursor = rows[-1]["snapshot_ts"] if rows else None

    return {
        "items": [dict(row) for row in rows],
        "next_cursor": next_cursor,
    }
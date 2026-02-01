from fastapi import APIRouter, Query
from google.cloud import bigquery
from typing import Optional

from bq import get_bq_client

router = APIRouter(
    prefix="/live-props",
    tags=["live-props"],
)

VIEW = "nba_live.v_live_player_props_enriched"
PERIOD_STATS_TABLE = "nba_goat_data.player_game_stats_period"


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
        WITH live AS (
            SELECT *
            FROM `{VIEW}`
            {where_sql}
        ),
        period_avgs AS (
            SELECT
                player_id,
                market,
                AVG(CASE WHEN period = 'Q3' THEN stat_value END) AS avg_q3,
                AVG(CASE WHEN period = 'Q4' THEN stat_value END) AS avg_q4,
                AVG(CASE WHEN period IN ('Q3', 'Q4') THEN stat_value END) AS avg_h2
            FROM (
                SELECT player_id, 'pts' AS market, pts AS stat_value, period
                FROM `{PERIOD_STATS_TABLE}`
                UNION ALL
                SELECT player_id, 'reb' AS market, reb AS stat_value, period
                FROM `{PERIOD_STATS_TABLE}`
                UNION ALL
                SELECT player_id, 'ast' AS market, ast AS stat_value, period
                FROM `{PERIOD_STATS_TABLE}`
                UNION ALL
                SELECT player_id, '3pm' AS market, fg3m AS stat_value, period
                FROM `{PERIOD_STATS_TABLE}`
                UNION ALL
                SELECT player_id, 'pra' AS market, (pts + reb + ast) AS stat_value, period
                FROM `{PERIOD_STATS_TABLE}`
                UNION ALL
                SELECT player_id, 'pr' AS market, (pts + reb) AS stat_value, period
                FROM `{PERIOD_STATS_TABLE}`
                UNION ALL
                SELECT player_id, 'pa' AS market, (pts + ast) AS stat_value, period
                FROM `{PERIOD_STATS_TABLE}`
                UNION ALL
                SELECT player_id, 'ra' AS market, (reb + ast) AS stat_value, period
                FROM `{PERIOD_STATS_TABLE}`
            )
            WHERE player_id IN (SELECT DISTINCT player_id FROM live)
            GROUP BY player_id, market
        )
        SELECT
            live.*,
            period_avgs.avg_q3,
            period_avgs.avg_q4,
            period_avgs.avg_h2
        FROM live
        LEFT JOIN period_avgs
            ON live.player_id = period_avgs.player_id
            AND live.market = period_avgs.market
        ORDER BY live.snapshot_ts DESC
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

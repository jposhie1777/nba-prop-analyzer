# debug_freshness.py
from fastapi import APIRouter, HTTPException
from google.cloud import bigquery

from bq import get_bq_client

router = APIRouter(
    prefix="/debug/freshness",
    tags=["debug"],
)

DATASETS = {
    "live_games": {
        "table": "graphite-flare-477419-h7.nba_live.live_games",
        "ts_col": "ingested_at",
    },
    "props": {
        "table": "nba_goat_data.props_mobile_v1",
        "ts_col": "updated_at",
    },
    "player_stats": {
        "table": "nba_goat_data.player_stats",
        "ts_col": "updated_at",
    },
}

@router.get("/{key}")
def get_freshness(key: str):
    cfg = DATASETS.get(key)
    if not cfg:
        raise HTTPException(status_code=404, detail="Unknown dataset")

    bq = get_bq_client()
    query = f"""
    SELECT
      COUNT(*) AS row_count,
      MAX({cfg["ts_col"]}) AS last_updated
    FROM `{cfg["table"]}`
    """

    rows = list(bq.query(query).result())
    row = rows[0]

    return {
        "row_count": row.row_count,
        "last_updated_ts": (
            row.last_updated.isoformat() if row.last_updated else None
        ),
    }

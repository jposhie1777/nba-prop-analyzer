from fastapi import APIRouter
from typing import Dict, Any
from google.cloud import bigquery

from lib.bq import get_bq_client  # 👈 USE YOUR EXISTING HELPER

router = APIRouter(prefix="/players", tags=["players"])

DATASET = "nba_goat_data"
TABLE = "v_player_season_mega"


@router.get("/season-mega")
def get_player_season_mega(limit: int = 500) -> Dict[str, Any]:
    """
    Raw read from v_player_season_mega.
    Frontend handles deduping for now.
    """

    client = get_bq_client()

    query = f"""
    SELECT *
    FROM `{DATASET}.{TABLE}`
    LIMIT @limit
    """

    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("limit", "INT64", limit)
            ]
        ),
    )

    rows = [dict(r) for r in job]

    return {
        "rows": rows,
        "count": len(rows),
        "source": f"{DATASET}.{TABLE}",
    }
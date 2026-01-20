from fastapi import APIRouter
from google.cloud import bigquery
from typing import List, Dict, Any
import os

router = APIRouter(prefix="/players", tags=["players"])

PROJECT_ID = os.getenv("GCP_PROJECT")
BQ_DATASET = "nba_goat_data"
TABLE = "v_player_season_mega"


def get_bq_client():
    return bigquery.Client(project=PROJECT_ID)


@router.get("/season-mega")
def get_player_season_mega(limit: int = 500) -> Dict[str, Any]:
    """
    Raw read from v_player_season_mega.
    Frontend handles deduping for now.
    """

    client = get_bq_client()

    query = f"""
    SELECT *
    FROM `{PROJECT_ID}.{BQ_DATASET}.{TABLE}`
    LIMIT @limit
    """

    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "limit", "INT64", limit
                )
            ]
        ),
    )

    rows = [dict(r) for r in job]

    return {
        "rows": rows,
        "count": len(rows),
        "source": TABLE,
    }
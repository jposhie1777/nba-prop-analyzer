from fastapi import APIRouter
from google.cloud import bigquery
from bq import get_bq_client

router = APIRouter(
    prefix="/live-props-dev",
    tags=["live-props-dev"],
)

@router.get("")
def read_live_props_dev(limit: int = 100):
    client = get_bq_client()

    query = """
    SELECT *
    FROM nba_dev.v_live_props_eligible_dev
    ORDER BY snapshot_ts DESC
    LIMIT @limit
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "limit", "INT64", limit
            )
        ]
    )

    job = client.query(query, job_config=job_config)

    return [dict(row) for row in job.result()]

# mobile_api/routes/bad_line_alerts.py

from fastapi import APIRouter
from google.cloud import bigquery

from bq import get_bq_client

router = APIRouter(
    prefix="/alerts",
    tags=["alerts"],
)

@router.get("/bad-lines")
def get_bad_lines(
    min_score: float = 0.75,
):
    client = get_bq_client()
    query = """
    SELECT *
    FROM `nba_live.v_bad_line_alerts_scored`
    WHERE is_bad_line = TRUE
      AND bad_line_score >= @min_score
    ORDER BY bad_line_score DESC
    """

    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "min_score", "FLOAT64", min_score
                )
            ]
        ),
    )

    return {
        "bad_lines": [dict(row) for row in job.result()]
    }
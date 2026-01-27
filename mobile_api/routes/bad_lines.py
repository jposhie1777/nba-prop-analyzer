# mobile_api/routes/bad_lines.py
from fastapi import APIRouter
from google.cloud import bigquery

router = APIRouter()
client = bigquery.Client()

@router.get("/bad-lines")
def get_bad_lines(
    min_score: float = 0.75,
    vendor: str | None = None,
):
    query = """
    SELECT *
    FROM `nba_live.v_bad_line_alerts_scored`
    WHERE bad_line_score >= @min_score
      AND is_bad_line = TRUE
    """

    params = [
        bigquery.ScalarQueryParameter("min_score", "FLOAT64", min_score),
    ]

    if vendor:
        query += " AND vendor = @vendor"
        params.append(
            bigquery.ScalarQueryParameter("vendor", "STRING", vendor)
        )

    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=params
        ),
    )

    rows = [dict(row) for row in job.result()]
    return {"bad_lines": rows}
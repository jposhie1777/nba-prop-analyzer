# dev_bq_routes.py
from fastapi import APIRouter, Query
from google.cloud import bigquery
import os
import json

router = APIRouter(prefix="/dev/bq", tags=["dev"])

PROJECT_ID = os.getenv("GCP_PROJECT")
bq = bigquery.Client(project=PROJECT_ID)

@router.get("/table-preview")
def preview_table(
    dataset: str = Query(...),
    table: str = Query(...),
):
    # 1️⃣ Columns
    cols_query = f"""
    SELECT column_name, data_type, is_nullable
    FROM `{PROJECT_ID}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = @table
    ORDER BY ordinal_position
    """
    cols_job = bq.query(
        cols_query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("table", "STRING", table)
            ]
        )
    )
    columns = [dict(r) for r in cols_job.result()]

    # 2️⃣ One example row (safe)
    row_query = f"""
    SELECT *
    FROM `{PROJECT_ID}.{dataset}.{table}`
    LIMIT 1
    """
    row_job = bq.query(row_query)
    rows = list(row_job.result())

    example_row = dict(rows[0]) if rows else None

    return {
        "dataset": dataset,
        "table": table,
        "columns": columns,
        "example_row": example_row,
    }
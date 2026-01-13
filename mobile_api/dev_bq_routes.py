from fastapi import APIRouter, Query
from google.cloud import bigquery

from bq import get_bq_client  # 👈 your existing helper

router = APIRouter(prefix="/dev/bq", tags=["dev"])

@router.get("/table-preview")
def preview_table(
    dataset: str = Query(...),
    table: str = Query(...),
):
    bq = get_bq_client()              # ✅ lazy, safe
    project_id = bq.project           # ✅ ALWAYS defined

    # 1️⃣ Columns (metadata only)
    cols_query = f"""
    SELECT column_name, data_type, is_nullable
    FROM `{project_id}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
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
    FROM `{project_id}.{dataset}.{table}`
    LIMIT 1
    """
    rows = list(bq.query(row_query).result())
    example_row = dict(rows[0]) if rows else None

    return {
        "dataset": dataset,
        "table": table,
        "columns": columns,
        "example_row": example_row,
    }
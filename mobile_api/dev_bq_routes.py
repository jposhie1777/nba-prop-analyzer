# db_bq_routes.py
# curl "https://pulse-mobile-api-763243624328.us-central1.run.app/dev/bq/table-preview?dataset=nba_goat_data&table=props_mobile_v1"
import os
from fastapi import APIRouter, Query
from google.cloud import bigquery
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import asyncio
from ingest_espn_player_headshots import run_headshot_ingest
from bq import get_bq_client

router = APIRouter(prefix="/dev/bq", tags=["dev"])

# ======================================================
# In-memory metadata cache (DEV ONLY)
# ======================================================
CACHE_TTL = timedelta(hours=2)

_metadata_cache: Dict[str, Dict] = {
    # dataset -> { tables, refreshed_at }
}

# ======================================================
# Helpers
# ======================================================
def is_stale(dataset: str) -> bool:
    entry = _metadata_cache.get(dataset)
    if not entry:
        return True
    return datetime.utcnow() - entry["refreshed_at"] > CACHE_TTL


def refresh_dataset_tables(dataset: str) -> List[Dict]:
    bq = get_bq_client()
    project_id = bq.project

    query = f"""
    SELECT table_name, table_type, row_count
    FROM `{project_id}.{dataset}.INFORMATION_SCHEMA.TABLES`
    WHERE table_type IN ('BASE TABLE', 'VIEW')
    ORDER BY table_name
    """

    rows = bq.query(query).result()
    tables = [
        {
            "name": r.table_name,
            "type": r.table_type,
            "row_count": r.row_count,
        }
        for r in rows
    ]

    _metadata_cache[dataset] = {
        "tables": tables,
        "refreshed_at": datetime.utcnow(),
    }

    return tables


# ======================================================
# List tables in a dataset (nba_goat_data, nba_live, etc.)
# ======================================================
@router.get("/datasets/{dataset}/tables")
def list_tables(dataset: str):
    if is_stale(dataset):
        refresh_dataset_tables(dataset)

    entry = _metadata_cache.get(dataset, {})
    return {
        "dataset": dataset,
        "tables": entry.get("tables", []),
        "last_refreshed": entry.get("refreshed_at"),
    }


# ======================================================
# Manual refresh endpoint (Cloud Scheduler / button)
# ======================================================
@router.post("/refresh-metadata")
def refresh_metadata(dataset: str = Query(...)):
    tables = refresh_dataset_tables(dataset)
    return {
        "dataset": dataset,
        "table_count": len(tables),
        "refreshed_at": datetime.utcnow().isoformat(),
    }


# ======================================================
# Table preview (schema + example row)
# ======================================================
@router.get("/table-preview")
def preview_table(
    dataset: str = Query(...),
    table: str = Query(...),
):
    bq = get_bq_client()
    project_id = bq.project

    # --------------------------
    # 1️⃣ Column metadata
    # --------------------------
    cols_query = f"""
    SELECT
      column_name,
      data_type,
      is_nullable
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

    # --------------------------
    # 2️⃣ Example row (FULL row)
    # --------------------------
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
        "column_count": len(columns),
        "columns": columns,
        "example_row": example_row,
    }    

# ======================================================
# Routines (stored procedures / functions)
# ======================================================
@router.get("/datasets/{dataset}/routines")
def list_routines(dataset: str):
    bq = get_bq_client()
    project_id = bq.project

    query = f"""
    SELECT routine_name, routine_type, routine_definition
    FROM `{project_id}.{dataset}.INFORMATION_SCHEMA.ROUTINES`
    ORDER BY routine_name
    """

    rows = bq.query(query).result()
    routines = [
        {
            "name": r.routine_name,
            "type": r.routine_type,
            "definition": r.routine_definition,
        }
        for r in rows
    ]

    return {"dataset": dataset, "routines": routines}
# ======================================================
# 🔴 Manual ESPN player headshot refresh (DEV ONLY)
# ======================================================
@router.post("/refresh-player-headshots")
async def refresh_player_headshots():
    if os.environ.get("RUNTIME_ENV") == "production":
        return {
            "task": "refresh_player_headshots",
            "status": "blocked",
            "reason": "Not allowed in production",
        }

    asyncio.create_task(
        asyncio.to_thread(run_headshot_ingest)
    )

    return {
        "task": "refresh_player_headshots",
        "status": "started",
        "started_at": datetime.utcnow().isoformat(),
    }

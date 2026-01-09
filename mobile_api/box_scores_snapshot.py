# box_scores_snapshot.py

from fastapi import APIRouter
from datetime import datetime, date, timezone
from zoneinfo import ZoneInfo
from typing import Dict, Any
import json
import os
import requests

from google.cloud import bigquery


# ======================================================
# Timezone (AUTHORITATIVE)
# ======================================================

NBA_TZ = ZoneInfo("America/New_York")


def nba_today() -> date:
    return datetime.now(NBA_TZ).date()


# ======================================================
# Auth / Config
# ======================================================

def require_api_key() -> str:
    key = os.environ.get("BALLDONTLIE_API_KEY")
    if not key:
        raise RuntimeError("BALLDONTLIE_API_KEY not set")
    return key


BQ_TABLE = "nba_live.box_scores_raw"

BASE_URL = "https://api.balldontlie.io/v1/box_scores/live"
TIMEOUT_SEC = 15


# ======================================================
# BigQuery client (LAZY)
# ======================================================

def get_bq_client() -> bigquery.Client:
    return bigquery.Client()


# ======================================================
# 🔥 CORE INGESTION FUNCTION (PURE)
# ======================================================

def run_box_scores_snapshot(
    *,
    game_date: date | None = None,
    per_page: int = 25,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Fetch live box scores from BallDontLie and snapshot raw JSON into BigQuery.

    - NO FastAPI dependencies
    - SAFE for background execution
    - SAFE for Cloud Run
    - SAFE for local dev
    """

    api_key = require_api_key()
    target_date = game_date or nba_today()

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
    }

    params = {
        "per_page": per_page,
    }

    # -------------------------------
    # Fetch from BallDontLie
    # -------------------------------
    resp = requests.get(
        BASE_URL,
        headers=headers,
        params=params,
        timeout=TIMEOUT_SEC,
    )
    resp.raise_for_status()

    payload = resp.json()

    # -------------------------------
    # Normalize JSON (BQ-safe)
    # -------------------------------
    payload_json = json.dumps(payload, default=str)

    row = {
        "snapshot_ts": datetime.now(timezone.utc).isoformat(),
        "game_date": target_date.isoformat(),
        "payload": payload_json,
    }

    # -------------------------------
    # Insert (unless dry run)
    # -------------------------------
    if not dry_run:
        client = get_bq_client()
        errors = client.insert_rows_json(BQ_TABLE, [row])

        if errors:
            raise RuntimeError(
                f"BigQuery insert errors: {errors}"
            )

    return {
        "status": "OK",
        "dry_run": dry_run,
        "game_date": target_date.isoformat(),
        "games": len(payload.get("data", [])),
        "snapshot_ts": row["snapshot_ts"],
    }


# ======================================================
# FastAPI Router (THIN WRAPPER)
# ======================================================

router = APIRouter(
    prefix="/debug",
    tags=["debug"],
)


@router.get("/box-scores/snapshot")
def snapshot_box_scores(
    game_date: date | None = None,
    per_page: int = 25,
    dry_run: bool = False,
):
    """
    Manual / HTTP-triggered snapshot endpoint.

    - Calls the same ingestion logic as background jobs
    - No duplicated logic
    """
    return run_box_scores_snapshot(
        game_date=game_date,
        per_page=per_page,
        dry_run=dry_run,
    )

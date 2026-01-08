#box_scores_snapshot.py
from fastapi import APIRouter
from datetime import datetime, date, timezone
import os
import requests
from google.cloud import bigquery
import json
from zoneinfo import ZoneInfo

NBA_TZ = ZoneInfo("America/New_York")

def nba_today() -> date:
    return datetime.now(NBA_TZ).date()

def require_api_key() -> str:
    key = os.environ.get("BALLDONTLIE_API_KEY")
    if not key:
        raise RuntimeError("BALLDONTLIE_API_KEY not set")
    return key
# ======================================================
# Router
# ======================================================
router = APIRouter(
    prefix="/debug",
    tags=["debug"],
)

# ======================================================
# Config
# ======================================================


PROJECT_ID = "graphite-flare-477419-h7"
BQ_TABLE = f"{PROJECT_ID}.nba_live.box_scores_raw"

BASE_URL = "https://api.balldontlie.io/v1/box_scores/live"
TIMEOUT_SEC = 15

# ======================================================
# BigQuery client
# ======================================================
bq = bigquery.Client()

# ======================================================
# Endpoint
# ======================================================
@router.get("/box-scores/snapshot")
def snapshot_box_scores(
    game_date: date | None = None,
    per_page: int = 25,
    dry_run: bool = False,
):
    api_key = require_api_key()
    # use api_key here
    """
    Manual snapshot of balldontlie box_scores.

    - Callable from browser or phone
    - Writes raw JSON to BigQuery
    - No parsing / no transforms
    """

    target_date = game_date or nba_today()

    params = {
        "per_page": per_page,
    }

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
    }

    # -------------------------------
    # Fetch from balldontlie
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
    # Prepare row
    # -------------------------------
    payload_clean = json.loads(
        json.dumps(payload, default=str)
    )

    row = {
        "snapshot_ts": datetime.now(timezone.utc).isoformat(),
        "game_date": target_date.isoformat(),
        "payload": json.dumps(payload),
    }

    # -------------------------------
    # Insert (unless dry run)
    # -------------------------------
    if not dry_run:
        errors = bq.insert_rows_json(BQ_TABLE, [row])
        if errors:
            return {
                "status": "ERROR",
                "errors": errors,
            }

    return {
        "status": "OK",
        "dry_run": dry_run,
        "game_date": target_date.isoformat(),
        "games": len(payload.get("data", [])),
        "snapshot_ts": row["snapshot_ts"],
    }
from fastapi import FastAPI
from datetime import datetime, date, timezone
import os
import requests
from google.cloud import bigquery

app = FastAPI()

# --------------------------------------------------
# Config
# --------------------------------------------------
BALLDONTLIE_API_KEY = os.environ["BALLDONTLIE_API_KEY"]
BQ_TABLE = "graphite-flare-477419-h7.nba_live.box_scores_raw"

client = bigquery.Client()

# --------------------------------------------------
# Manual snapshot endpoint
# --------------------------------------------------
@app.get("/debug/box-scores/snapshot")
def snapshot_box_scores(game_date: str | None = None):
    """
    Manually pull balldontlie box_scores and store raw JSON in BigQuery.
    Trigger via browser / phone / curl.
    """

    target_date = game_date or date.today().isoformat()

    resp = requests.get(
        "https://api.balldontlie.io/v1/box_scores",
        params={
            "date": target_date,
            "per_page": 50,
        },
        headers={
            "Authorization": f"Bearer {BALLDONTLIE_API_KEY}",
            "Accept": "application/json",
        },
        timeout=15,
    )
    resp.raise_for_status()

    payload = resp.json()

    row = {
        "snapshot_ts": datetime.now(timezone.utc),
        "game_date": target_date,
        "payload": payload,
    }

    errors = client.insert_rows_json(BQ_TABLE, [row])
    if errors:
        return {
            "status": "ERROR",
            "errors": errors,
        }

    return {
        "status": "OK",
        "game_date": target_date,
        "games": len(payload.get("data", [])),
        "snapshot_ts": row["snapshot_ts"].isoformat(),
    }

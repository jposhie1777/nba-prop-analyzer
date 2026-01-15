import os
import requests
from datetime import datetime, timezone

from bq import get_bq_client
from season_average_combos import PLAYER_SEASON_AVERAGE_COMBOS

PROJECT_ID = os.getenv("GCP_PROJECT")
DATASET = "nba_goat_data"
TABLE = "player_season_averages_raw"

BASE_URL = "https://api.balldontlie.io/nba/v1/season_averages"
API_KEY = os.getenv("BALLDONTLIE_API_KEY")

DEFAULT_SEASON = 2025
DEFAULT_SEASON_TYPE = "regular"

def ingest_player_season_averages(
    season: int = DEFAULT_SEASON,
    season_type: str = DEFAULT_SEASON_TYPE,
):
    bq = get_bq_client()
    table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"

    rows_to_insert = []

    # ✅ CORRECT iteration for LIST OF TUPLES
    for category, stat_type in PLAYER_SEASON_AVERAGE_COMBOS:
        url = f"{BASE_URL}/{category}"
        params = {
            "season": season,
            "season_type": season_type,
            "type": stat_type,
        }
    
        resp = requests.get(
            url,
            headers={"Authorization": API_KEY},
            params=params,
            timeout=30,
        )
        resp.raise_for_status()
    
        data = resp.json().get("data", [])
    
        for record in data:
            rows_to_insert.append({
                "ingested_at": datetime.now(timezone.utc).isoformat(),
                "season": season,
                "season_type": season_type,
                "category": category,
                "type": stat_type,
                "player_id": record.get("player_id"),
    
                # ✅ MUST be RECORD for BigQuery STRUCT
                "payload": record,
            })

    if rows_to_insert:
        errors = bq.insert_rows_json(table_id, rows_to_insert)
        if errors:
            raise RuntimeError(f"BigQuery insert errors: {errors}")

    return {
        "status": "ok",
        "rows_inserted": len(rows_to_insert),
        "season": season,
        "season_type": season_type,
    }
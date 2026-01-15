# ingest/season/player/shooting.py
import json
from mobile_api.ingest.common.bq import get_bq_client
from mobile_api.ingest.common.http import get
from mobile_api.ingest.common.logging import now_ts

TABLE = "nba_goat_data.player_season_shooting"
URL = "https://api.balldontlie.io/nba/v1/season_averages/shooting"

TYPES = [
    "overall",
    "by_zone",
    "5ft_range",
    "less_than_10ft",
    "less_than_6ft",
]

def run(season: int, season_type: str):
    bq = get_bq_client()
    rows = []

    for t in TYPES:
        resp = get(URL, {
            "season": season,
            "season_type": season_type,
            "type": t,
        })

        if resp.status_code == 400:
            print(f"⚠️ skipping shooting:{t}")
            continue

        resp.raise_for_status()

        for r in resp.json().get("data", []):
            rows.append({
                "ingested_at": now_ts(),
                "season": season,
                "season_type": season_type,
                "type": t,
                "player_id": r["player"]["id"],
                "payload": json.dumps(r),
            })

    if rows:
        bq.insert_rows_json(TABLE, rows)
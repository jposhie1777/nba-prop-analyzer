# ingest/season/league/general
import json
from mobile_api.ingest.common.bq import get_bq_client
from mobile_api.ingest.common.http import get
from mobile_api.ingest.common.players import get_active_player_ids
from mobile_api.ingest.common.logging import now_ts

TABLE = "nba_goat_data.league_season_general"
URL = "https://api.balldontlie.io/nba/v1/season_averages/general"

TYPES = [
    "base",
    "advanced",
    "usage",
    "scoring",
    "defense",
    "misc",
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
        resp.raise_for_status()

        for r in resp.json().get("data", []):
            rows.append({
                "ingested_at": now_ts(),
                "season": season,
                "season_type": season_type,
                "type": t,
                "payload": json.dumps(r),
            })

    if rows:
        bq.insert_rows_json(TABLE, rows)
# ingest/season/league/hustle
import json
from mobile_api.ingest.common.bq import get_bq_client
from mobile_api.ingest.common.http import get
from mobile_api.ingest.common.players import get_active_player_ids
from mobile_api.ingest.common.logging import now_ts

TABLE = "nba_goat_data.league_season_hustle"
URL = "https://api.balldontlie.io/nba/v1/season_averages/hustle"

def run(season: int, season_type: str):
    bq = get_bq_client()
    resp = get(URL, {
        "season": season,
        "season_type": season_type,
    })
    resp.raise_for_status()

    rows = [{
        "ingested_at": now_ts(),
        "season": season,
        "season_type": season_type,
        "payload": json.dumps(r),
    } for r in resp.json().get("data", [])]

    if rows:
        bq.insert_rows_json(TABLE, rows)
TYPE = "cut"
TABLE = "nba_goat_data.player_playtype_cut"

import json
from ingest.common.bq import get_bq_client
from ingest.common.http import get
from ingest.common.players import get_active_player_ids
from ingest.common.logging import now_ts

URL = "https://api.balldontlie.io/nba/v1/season_averages/playtype"

def run(season: int, season_type: str):
    bq = get_bq_client()
    players = get_active_player_ids()

    resp = get(URL, {
        "season": season,
        "season_type": season_type,
        "type": TYPE,
        "player_ids[]": players,
    })
    resp.raise_for_status()

    rows = [{
        "ingested_at": now_ts(),
        "season": season,
        "season_type": season_type,
        "player_id": r["player"]["id"],
        "payload": json.dumps(r),
    } for r in resp.json().get("data", [])]

    if rows:
        bq.insert_rows_json(TABLE, rows)
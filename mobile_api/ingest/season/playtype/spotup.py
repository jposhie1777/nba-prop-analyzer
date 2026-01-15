TYPE = "spotup"
TABLE = "nba_goat_data.player_playtype_spotup"

import json
from mobile_api.ingest.common.bq import get_bq_client
from mobile_api.ingest.common.http import get
from mobile_api.ingest.common.players import get_active_player_ids
from mobile_api.ingest.common.logging import now_ts
from mobile_api.ingest.common.batch import chunked

URL = "https://api.balldontlie.io/nba/v1/season_averages/playtype"

def run(season: int, season_type: str):
    bq = get_bq_client()
    players = get_active_player_ids()
    rows = []

    for batch in chunked(players, size=10):
        resp = get(URL, {
            "season": season,
            "season_type": season_type,
            "type": TYPE,
            "player_ids[]": batch,
        })

        if resp.status_code == 400:
            print(f"⚠️ skipping batch size={len(batch)} for playtype:{TYPE}")
            continue

        resp.raise_for_status()

        for r in resp.json().get("data", []):
            rows.append({
                "ingested_at": now_ts(),
                "season": season,
                "season_type": season_type,
                "player_id": r["player"]["id"],
                "payload": json.dumps(r),
            })

    if rows:
        bq.insert_rows_json(TABLE, rows)
from datetime import datetime, timezone
import json
import requests

from live_odds_common import (
    BDL_V2,
    TIMEOUT_SEC,
    require_api_key,
    get_bq_client,
    fetch_live_game_ids,
)

BQ_TABLE = "graphite-flare-477419-h7.nba_live.live_player_prop_odds_raw"

def ingest_live_player_prop_odds() -> dict:
    """
    Pull live player prop odds ONLY for LIVE games.
    """

    live_game_ids = fetch_live_game_ids()
    if not live_game_ids:
        return {"status": "SKIPPED", "reason": "no live games"}

    headers = {
        "Authorization": f"Bearer {require_api_key()}",
        "Accept": "application/json",
    }

    now = datetime.now(timezone.utc)
    rows = []

    for game_id in live_game_ids:
        resp = requests.get(
            f"{BDL_V2}/odds/player_props",
            headers=headers,
            params={"game_id": game_id},
            timeout=TIMEOUT_SEC,
        )
        resp.raise_for_status()

        payload = resp.json()

        rows.append(
            {
                "snapshot_ts": now.isoformat(),
                "game_id": game_id,
                "payload": json.dumps(payload),
            }
        )

    if rows:
        client = get_bq_client()
        errors = client.insert_rows_json(BQ_TABLE, rows)
        if errors:
            raise RuntimeError(f"Player prop odds insert errors: {errors}")

    return {
        "status": "OK",
        "games": len(rows),
        "snapshot_ts": now.isoformat(),
    }
# live_game_odds_ingest.py
from datetime import datetime, timezone
import json
import requests
from live_odds_common import LIVE_ODDS_BOOKS

from live_odds_common import (
    BDL_V2,
    TIMEOUT_SEC,
    require_api_key,
    get_bq_client,
    fetch_live_game_ids,
)

BQ_TABLE = "graphite-flare-477419-h7.nba_live.live_game_odds_raw"

def ingest_live_game_odds() -> dict:
    """
    Pull live betting odds ONLY for LIVE games.
    Safe for background execution.
    """

    live_game_ids = fetch_live_game_ids()
    if not live_game_ids:
        return {"status": "SKIPPED", "reason": "no live games"}

    headers = {
        "Authorization": f"Bearer {require_api_key()}",
        "Accept": "application/json",
    }

    resp = requests.get(
        f"{BDL_V2}/odds",
        headers=headers,
        params={"game_ids[]": live_game_ids},
        timeout=TIMEOUT_SEC,
    )
    resp.raise_for_status()

    payload = resp.json()
    now = datetime.now(timezone.utc)

    rows = []
    for game in payload.get("data", []):
        book = game.get("book")
    
        if book not in LIVE_ODDS_BOOKS:
            continue
    
        rows.append(
            {
                "snapshot_ts": now.isoformat(),
                "game_id": game.get("game_id"),
                "payload": json.dumps(game),
            }
        )

    if rows:
        client = get_bq_client()
        errors = client.insert_rows_json(BQ_TABLE, rows)
        if errors:
            raise RuntimeError(f"Game odds insert errors: {errors}")

    return {
        "status": "OK",
        "games": len(rows),
        "snapshot_ts": now.isoformat(),
    }
# live_player_prop_odds_ingest.py

from datetime import datetime, timezone
import json
import requests

from live_odds_common import (
    BDL_V2,
    TIMEOUT_SEC,
    require_api_key,
    get_bq_client,
    fetch_live_game_ids,
    LIVE_PLAYER_PROP_MARKETS,
    LIVE_ODDS_BOOKS,
    normalize_book,
)

BQ_TABLE = "graphite-flare-477419-h7.nba_live.live_player_prop_odds_raw"


def ingest_live_player_prop_odds() -> dict:
    """
    Pull live player prop odds ONLY for:
    - LIVE games
    - DraftKings / FanDuel
    - PTS / AST / REB / 3PM
    """

    live_game_ids = fetch_live_game_ids()
    if not live_game_ids:
        return {"status": "SKIPPED", "reason": "no live games"}

    headers = {
        "Authorization": f"Bearer {require_api_key()}",
        "Accept": "application/json",
    }

    now = datetime.now(timezone.utc)
    client = get_bq_client()
    games_written = 0

    for game_id in live_game_ids:
        resp = requests.get(
            f"{BDL_V2}/odds/player_props",
            headers=headers,
            params={"game_id": game_id},
            timeout=TIMEOUT_SEC,
        )
        resp.raise_for_status()

        payload = resp.json()

        # ----------------------------------
        # Filter markets + sportsbooks
        # ----------------------------------
        filtered_markets = []

        for market in payload.get("data", []):
            market_key = market.get("market")
            book = normalize_book(market.get("book"))

            # Hard filters
            if book not in LIVE_ODDS_BOOKS:
                continue

            if market_key not in LIVE_PLAYER_PROP_MARKETS:
                continue

            filtered_markets.append(market)

        # Nothing we care about for this game
        if not filtered_markets:
            continue

        row = {
            "snapshot_ts": now.isoformat(),
            "game_id": game_id,
            "payload": json.dumps(
                {
                    "game_id": game_id,
                    "markets": filtered_markets,
                }
            ),
        }

        errors = client.insert_rows_json(BQ_TABLE, [row])
        if errors:
            raise RuntimeError(f"Player prop odds insert errors: {errors}")

        games_written += 1

    return {
        "status": "OK",
        "games_written": games_written,
        "snapshot_ts": now.isoformat(),
    }
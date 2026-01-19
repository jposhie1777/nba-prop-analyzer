# live_player_prop_odds_ingest.py

from datetime import datetime, timezone
import json
import requests

from LiveOdds.live_odds_common import (
    BDL_V2,
    TIMEOUT_SEC,
    require_api_key,
    get_bq_client,
    fetch_live_game_ids,
    LIVE_PLAYER_PROP_MARKETS,
    LIVE_ODDS_BOOKS,
    normalize_book,
)

# ======================================================
# Market normalization (vendor → canonical)
# ======================================================
MARKET_NORMALIZATION = {
    "points": "pts",
    "assists": "ast",
    "rebounds": "reb",

    # --- 3PT markets ---
    "three_pointers_made": "3pm",
    "fg3m": "3pm",
    "threes": "3pm",
}

BQ_TABLE = "graphite-flare-477419-h7.nba_live.live_player_prop_odds_raw"


def ingest_live_player_prop_odds() -> dict:
    """
    Pull live player prop odds ONLY for:
    - LIVE games
    - DraftKings / FanDuel
    - PTS / AST / REB / 3PM
    - Over/Under + Milestones
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

        filtered_markets = []

        for item in payload.get("data", []):
            raw_prop_type = item.get("prop_type")
            prop_type = MARKET_NORMALIZATION.get(raw_prop_type, raw_prop_type)

            raw_book = item.get("book") or item.get("sportsbook")
            book = normalize_book(raw_book)


            # -----------------------------
            # Hard filters
            # -----------------------------
            if prop_type not in LIVE_PLAYER_PROP_MARKETS:
                continue

            if book not in LIVE_ODDS_BOOKS:
                continue

            market_data = item.get("market") or {}
            market_type = market_data.get("type")

            line_value = (
                float(item.get("line_value"))
                if item.get("line_value") is not None
                else None
            )

            # -----------------------------
            # OVER / UNDER
            # -----------------------------
            if market_type == "over_under":
                filtered_markets.append(
                    {
                        "player_id": item.get("player_id"),
                        "market": prop_type,
                        "market_type": "over_under",
                        "line": line_value,
                        "book": book,
                        "odds": {
                            "over": market_data.get("over_odds"),
                            "under": market_data.get("under_odds"),
                        },
                    }
                )
                continue

            # -----------------------------
            # MILESTONE (X+)
            # -----------------------------
            if market_type == "milestone":
                filtered_markets.append(
                    {
                        "player_id": item.get("player_id"),
                        "market": prop_type,
                        "market_type": "milestone",
                        "line": line_value,  # milestone number (e.g. 20, 25)
                        "book": book,
                        "odds": {
                            "yes": market_data.get("odds")
                        },
                    }
                )
                continue

            # Ignore anything else (alternate lines, exotics, etc.)

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
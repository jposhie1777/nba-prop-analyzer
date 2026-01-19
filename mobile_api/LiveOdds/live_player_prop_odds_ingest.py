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
    normalize_book,
)

BQ_TABLE = "graphite-flare-477419-h7.nba_live.live_player_prop_odds_raw"


def ingest_live_player_prop_odds() -> dict:
    """
    Ingest ALL exposed live player prop odds from BDL.

    Scope:
    - LIVE games only
    - ALL books
    - ALL prop types (points, assists, PRA, turnovers, etc.)
    - ALL market types (over_under, milestone, alternates, specials, etc.)

    No filtering. No assumptions.
    Downstream views decide what matters.
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

    rows_to_insert = []
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
        items = payload.get("data", [])

        if not items:
            continue

        # Store EXACTLY what BDL gives us, with light normalization helpers
        row = {
            "snapshot_ts": now.isoformat(),
            "game_id": game_id,
            "payload": json.dumps(
                {
                    "game_id": game_id,
                    "count": len(items),
                    "ingested_at": now.isoformat(),
                    "items": [
                        {
                            **item,
                            # helpers for downstream querying
                            "normalized_book": normalize_book(item.get("vendor")),
                            "prop_type": item.get("prop_type"),
                            "market_type": (item.get("market") or {}).get("type"),
                        }
                        for item in items
                    ],
                }
            ),
        }

        rows_to_insert.append(row)
        games_written += 1

    if rows_to_insert:
        errors = client.insert_rows_json(BQ_TABLE, rows_to_insert)
        if errors:
            raise RuntimeError(f"Player prop odds insert errors: {errors}")

    return {
        "status": "OK",
        "games_written": games_written,
        "rows_inserted": len(rows_to_insert),
        "snapshot_ts": now.isoformat(),
    }

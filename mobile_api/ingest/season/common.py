import json
from typing import List
from mobile_api.ingest.common.bq import get_bq_client
from mobile_api.ingest.common.http import get
from mobile_api.ingest.common.logging import now_ts

BASE_URL = "https://api.balldontlie.io/nba/v1/season_averages"

def ingest_category(
    *,
    table: str,
    category: str,
    types: List[str],
    season: int,
    season_type: str,
):
    """
    Generic BallDontLie season_averages ingester.
    Enforces valid category/type pairing by construction.
    """
    bq = get_bq_client()
    rows = []

    url = f"{BASE_URL}/{category}"

    for t in types:
        resp = get(url, {
            "season": season,
            "season_type": season_type,
            "type": t,
        })

        if resp.status_code == 400:
            print(f"⚠️ skipping {category}:{t}")
            continue

        resp.raise_for_status()

        for r in resp.json().get("data", []):
            rows.append({
                "ingested_at": now_ts(),
                "season": season,
                "season_type": season_type,
                "category": category,
                "type": t,
                "player_id": r["player"]["id"],
                "payload": json.dumps(r),
            })

    if rows:
        bq.insert_rows_json(table, rows)
        print(f"✅ inserted {len(rows)} rows → {table}")
    else:
        print(f"ℹ️ no rows for {category}")
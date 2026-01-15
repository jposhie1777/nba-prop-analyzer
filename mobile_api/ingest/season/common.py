# ingest/season/common.py

import json
import time
from typing import List, Optional

from mobile_api.ingest.common.bq import get_bq_client
from mobile_api.ingest.common.http import get
from mobile_api.ingest.common.logging import now_ts

BASE_URL = "https://api.balldontlie.io/nba/v1/season_averages"

REQUEST_SLEEP = 0.25  # polite throttling


def ingest_category(
    *,
    table: str,
    category: str,
    types: List[str],
    season: int,
    season_type: str,
    player_ids: List[int],
    run_ts,
):
    """
    PLAYER season_averages ingester (single batch).

    Assumptions:
    - player_ids length is safely under API evaluation cap
    - run_ts is generated ONCE per batch (by run_all)
    """

    if not player_ids:
        print("⚠️ empty player batch, skipping")
        return

    bq = get_bq_client()
    url = f"{BASE_URL}/{category}"
    total_rows = 0

    for stat_type in types:
        print(f"→ ingesting player {category}:{stat_type} ({len(player_ids)} players)")
        rows = []

        params = {
            "season": season,
            "season_type": season_type,
            "type": stat_type,
            "player_ids[]": player_ids,
        }

        resp = get(url, params)

        # Invalid category/type pairing
        if resp.status_code == 400:
            print(f"⚠️ skipping invalid {category}:{stat_type}")
            continue

        resp.raise_for_status()

        data = resp.json().get("data", [])

        for r in data:
            player = r.get("player")
            if not player:
                continue

            rows.append(
                {
                    "run_ts": run_ts,
                    "ingested_at": now_ts(),
                    "season": season,
                    "season_type": season_type,
                    "category": category,
                    "type": stat_type,
                    "player_id": player["id"],
                    "payload": json.dumps(r),
                }
            )

        if rows:
            errors = bq.insert_rows_json(table, rows)

            if errors:
                print("❌ BigQuery insert errors:")
                for e in errors[:5]:
                    print(e)
                raise RuntimeError(
                    f"BigQuery insert failed for {category}:{stat_type}"
                )

            total_rows += len(rows)
            print(f"✅ player {category}:{stat_type} → {len(rows)} rows")
        else:
            print(f"ℹ️ no rows for {category}:{stat_type}")

        time.sleep(REQUEST_SLEEP)

    print(f"🏁 inserted {total_rows} rows → {table}")

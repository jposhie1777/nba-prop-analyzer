# ingest/season/team/common.py

import json
import time
from typing import Optional

from mobile_api.ingest.common.bq import get_bq_client
from mobile_api.ingest.common.http import get
from mobile_api.ingest.common.logging import now_ts

BASE_URL = "https://api.balldontlie.io/nba/v1/team_season_averages"

# Teams are ALWAYS <= 30, so we do NOT need true pagination
PER_PAGE = 100          # fetch everything in one request
REQUEST_SLEEP = 0.25    # be nice to the API (4 req/sec max)


def ingest_team_category(
    *,
    table: str,
    category: str,
    season: int,
    season_type: str,
    stat_type: Optional[str] = None,
):
    """
    Ingest one TEAM season averages category/type.
    Team endpoints are small (<=30 rows), so we fetch in one request.
    """

    print(f"→ ingesting team {category}:{stat_type or 'none'}")

    bq = get_bq_client()
    rows = []

    url = f"{BASE_URL}/{category}"

    params = {
        "season": season,
        "season_type": season_type,
        "page": 1,
        "per_page": PER_PAGE,
    }

    if stat_type:
        params["type"] = stat_type

    # ---- HTTP request ----
    resp = get(url, params)
    resp.raise_for_status()

    payload = resp.json()
    data = payload.get("data", [])

    if not data:
        print(f"↪ no data for team {category}:{stat_type or 'none'}")
        return

    for r in data:
        team = r.get("team")
        if not team:
            continue

        rows.append({
            "ingested_at": now_ts(),
            "season": season,
            "season_type": season_type,
            "team_id": team["id"],
            "team_abbreviation": team["abbreviation"],
            "team_name": team["full_name"],
            "category": category,
            "type": stat_type,
            "payload": json.dumps(r),
        })

    if rows:
        bq.insert_rows_json(table, rows)
        print(f"✅ team {category}:{stat_type or 'none'} → {len(rows)} rows")

    # ---- throttle ----
    time.sleep(REQUEST_SLEEP)

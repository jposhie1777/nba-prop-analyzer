# ingest_season_averages.py
import os
import time
import requests
import json
from typing import Dict, List, Optional
from datetime import datetime, timezone

from google.cloud import bigquery
from google.cloud.bigquery import json as bq_json   # ⭐ ADD THIS
from bq import get_bq_client


# ======================================================
# CONFIG
# ======================================================

BQ_DATASET = "nba_goat_data"
BASE_URL = "https://api.balldontlie.io/nba/v1"

HEADERS = None
bq: bigquery.Client | None = None

def get_headers() -> dict:
    api_key = os.getenv("BALLDONTLIE_API_KEY")
    if not api_key:
        raise RuntimeError("❌ BALLDONTLIE_API_KEY not set")
    return {
        "Authorization": api_key
    }

SEASON_AVERAGES_MATRIX = {
    "general": [
        "base", "advanced", "usage", "scoring",
        "misc", "opponent", "defense", "violations"
    ],
    "clutch": ["base", "advanced", "misc", "scoring"],
    "shooting": [
        "by_zone_base", "by_zone_opponent",
        "5ft_range_base", "5ft_range_opponent"
    ],
    "playtype": [
        "cut", "handoff", "isolation", "offrebound",
        "offscreen", "postup", "prballhandler",
        "prrollman", "spotup", "transition", "misc"
    ],
    "tracking": [
        "painttouch", "efficiency", "speeddistance",
        "defense", "elbowtouch", "posttouch",
        "passing", "drives", "rebounding",
        "catchshoot", "pullupshot", "possessions"
    ],
    "hustle": [None],
    "shotdashboard": [
        "overall", "pullups", "catch_and_shoot",
        "less_than_10_ft"
    ]
}

def get_current_nba_season() -> int:
    """
    Returns the NBA season start year.
    Example:
      Oct 2025 – Jun 2026 -> 2025
      Oct 2024 – Jun 2025 -> 2024
    """
    today = datetime.now(timezone.utc)
    return today.year if today.month >= 10 else today.year - 1


SEASON = int(os.getenv("SEASON", get_current_nba_season()))
SEASON_TYPE = os.getenv("SEASON_TYPE", "regular")

REQUEST_SLEEP = 0.25
NOW_TS = datetime.now(timezone.utc).isoformat()


# ======================================================
# HELPERS
# ======================================================
def safe_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

def fetch_all_pages(url: str, params: Dict) -> List[Dict]:
    rows = []

    while True:
        r = requests.get(url, headers=HEADERS, params=params)

        if r.status_code == 400:
            print(f"⚠️ Unsupported season averages endpoint: {r.url}")
            return []

        r.raise_for_status()
        payload = r.json()

        rows.extend(payload.get("data", []))

        cursor = payload.get("meta", {}).get("cursor")
        if not cursor:
            break

        params["cursor"] = cursor
        time.sleep(REQUEST_SLEEP)

    return rows



def insert_rows(table: str, rows: List[Dict]):
    if not rows:
        return

    print(f"📤 Attempting BigQuery insert: {table} ({len(rows)} rows)")

    errors = bq.insert_rows(table, rows)

    if errors:
        print("❌ BigQuery insert errors:", errors)
        raise RuntimeError(errors)

    print(f"✅ BigQuery insert successful: {table}")



def log_ingestion(
    scope: str,
    category: str,
    type_: Optional[str],
    rows_ingested: int,
    success: bool,
    error_message: Optional[str] = None,
):
    insert_rows(
        f"{BQ_DATASET}.season_averages_ingestion_log",
        [{
            "season": SEASON,
            "season_type": SEASON_TYPE,
            "scope": scope,
            "category": category,
            "type": type_,
            "rows_ingested": rows_ingested,
            "success": success,
            "error_message": error_message,
            "ingested_at": NOW_TS,
        }]
    )

# ======================================================
# INGEST PLAYER SEASON AVERAGES
# ======================================================

def ingest_player():
    for category, types in SEASON_AVERAGES_MATRIX.items():
        for type_ in types:
            try:
                params = {
                    "season": SEASON,
                    "season_type": SEASON_TYPE,
                    "per_page": 100,
                }
                if type_:
                    params["type"] = type_

                url = f"{BASE_URL}/season_averages/{category}"
                rows = fetch_all_pages(url, params)

                raw_rows = []
                stat_rows = []

                for r in rows:
                    player_id = r["player"]["id"]

                    raw_rows.append({
                        "season": SEASON,
                        "season_type": SEASON_TYPE,
                        "category": category,
                        "type": type_,
                        "player_id": player_id,
                        "payload": bq_json.Json(r),
                        "ingested_at": NOW_TS,
                    })

                    for k, v in r.get("stats", {}).items():
                        num = safe_float(v)
                        if num is None:
                            continue
                        stat_rows.append({
                            "season": SEASON,
                            "season_type": SEASON_TYPE,
                            "category": category,
                            "type": type_,
                            "player_id": player_id,
                            "stat_key": k,
                            "stat_value": num,
                            "ingested_at": NOW_TS,
                        })

                insert_rows(f"{BQ_DATASET}.raw_season_averages_player", raw_rows)
                insert_rows(f"{BQ_DATASET}.season_averages_player_stats", stat_rows)

                print(
                    f"✅ Player season averages inserted: "
                    f"{category} | {type_ or 'default'} | "
                    f"raw={len(raw_rows)} stats={len(stat_rows)}"
                )

                log_ingestion("player", category, type_, len(rows), True)


            except Exception as e:
                log_ingestion("player", category, type_, 0, False, str(e))

# ======================================================
# INGEST TEAM SEASON AVERAGES
# ======================================================

def ingest_team():
    for category, types in SEASON_AVERAGES_MATRIX.items():
        for type_ in types:
            try:
                params = {
                    "season": SEASON,
                    "season_type": SEASON_TYPE,
                    "per_page": 100,
                }
                if type_:
                    params["type"] = type_

                url = f"{BASE_URL}/team_season_averages/{category}"
                rows = fetch_all_pages(url, params)

                raw_rows = []
                stat_rows = []

                for r in rows:
                    team_id = r["team"]["id"]

                    raw_rows.append({
                        "season": SEASON,
                        "season_type": SEASON_TYPE,
                        "category": category,
                        "type": type_,
                        "team_id": team_id,
                        "payload": bq_json.Json(r),
                        "ingested_at": NOW_TS,
                    })

                    for k, v in r.get("stats", {}).items():
                        num = safe_float(v)
                        if num is None:
                            continue
                        stat_rows.append({
                            "season": SEASON,
                            "season_type": SEASON_TYPE,
                            "category": category,
                            "type": type_,
                            "team_id": team_id,
                            "stat_key": k,
                            "stat_value": num,
                            "ingested_at": NOW_TS,
                        })

                insert_rows(f"{BQ_DATASET}.raw_season_averages_team", raw_rows)
                insert_rows(f"{BQ_DATASET}.season_averages_team_stats", stat_rows)

                print(
                    f"✅ Team season averages inserted: "
                    f"{category} | {type_ or 'default'} | "
                    f"raw={len(raw_rows)} stats={len(stat_rows)}"
                )

                log_ingestion("team", category, type_, len(rows), True)


            except Exception as e:
                log_ingestion("team", category, type_, 0, False, str(e))

# ======================================================
# MAIN
# ======================================================

def main():
    global bq, HEADERS

    print(f"🚀 Starting season averages ingest {SEASON} {SEASON_TYPE}")
    print(f"🏀 Using NBA season start year: {SEASON}")

    bq = get_bq_client()     # ADC + env fallback
    HEADERS = get_headers()  # strict API key validation

    ingest_player()
    ingest_team()

    print("✅ Season averages ingestion complete")


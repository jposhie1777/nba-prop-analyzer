# ingest_season_averages.py
import os
import time
import requests
from typing import Dict, List, Optional
from google.cloud import bigquery
from datetime import datetime, timezone

# ======================================================
# CONFIG
# ======================================================

def get_project_id() -> str:
    project_id = (
        os.getenv("GCP_PROJECT")
        or os.getenv("GOOGLE_CLOUD_PROJECT")
    )
    if not project_id:
        raise RuntimeError("❌ GCP_PROJECT / GOOGLE_CLOUD_PROJECT not set")
    return project_id

BQ_DATASET = "nba_goat_data"
BASE_URL = "https://api.balldontlie.io/nba/v1"
HEADERS = None
bq = None

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

SEASON = int(os.getenv("SEASON", "2024"))
SEASON_TYPE = os.getenv("SEASON_TYPE", "regular")

REQUEST_SLEEP = 0.25  # rate-limit safety

NOW_TS = datetime.now(timezone.utc)

# ======================================================
# HELPERS
# ======================================================

def fetch_all_pages(url: str, params: Dict) -> List[Dict]:
    rows = []
    while True:
        r = requests.get(url, headers=HEADERS, params=params)
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
    errors = bq.insert_rows_json(table, rows)
    if errors:
        raise RuntimeError(errors)


def log_ingestion(
    scope: str,
    category: str,
    type_: Optional[str],
    rows_ingested: int,
    success: bool,
    error_message: Optional[str] = None
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
            "ingested_at": NOW_TS
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
                    "per_page": 100
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
                        "payload": r,
                        "ingested_at": NOW_TS
                    })

                    for k, v in r.get("stats", {}).items():
                        stat_rows.append({
                            "season": SEASON,
                            "season_type": SEASON_TYPE,
                            "category": category,
                            "type": type_,
                            "player_id": player_id,
                            "stat_key": k,
                            "stat_value": float(v) if v is not None else None,
                            "ingested_at": NOW_TS
                        })

                insert_rows(f"{BQ_DATASET}.raw_season_averages_player", raw_rows)
                insert_rows(f"{BQ_DATASET}.season_averages_player_stats", stat_rows)

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
                    "per_page": 100
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
                        "payload": r,
                        "ingested_at": NOW_TS
                    })

                    for k, v in r.get("stats", {}).items():
                        stat_rows.append({
                            "season": SEASON,
                            "season_type": SEASON_TYPE,
                            "category": category,
                            "type": type_,
                            "team_id": team_id,
                            "stat_key": k,
                            "stat_value": float(v) if v is not None else None,
                            "ingested_at": NOW_TS
                        })

                insert_rows(f"{BQ_DATASET}.raw_season_averages_team", raw_rows)
                insert_rows(f"{BQ_DATASET}.season_averages_team_stats", stat_rows)

                log_ingestion("team", category, type_, len(rows), True)

            except Exception as e:
                log_ingestion("team", category, type_, 0, False, str(e))

# ======================================================
# MAIN
# ======================================================

def main(season: int, season_type: str):
    global SEASON, SEASON_TYPE, bq, HEADERS

    SEASON = season
    SEASON_TYPE = season_type

    project_id = get_project_id()
    bq = bigquery.Client(project=project_id)

    HEADERS = get_headers()

    ingest_player()
    ingest_team()
    print("✅ Season averages ingestion complete")

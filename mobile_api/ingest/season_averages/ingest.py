# mobile_api/ingest/season_averages/ingest.py
"""
Season Averages Ingestion Module

Fetches player and team season averages from Balldontlie API.
Supports all category/type combinations documented in the API.

Player Season Averages:
    - GET https://api.balldontlie.io/v1/season_averages/{category}

Team Season Averages:
    - GET https://api.balldontlie.io/v1/team_season_averages/{category}
"""

import json
import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from google.cloud import bigquery

# ======================================================
# Configuration
# ======================================================

BDL_BASE_V1 = "https://api.balldontlie.io/v1"
NBA_TZ = ZoneInfo("America/New_York")
UTC_TZ = ZoneInfo("UTC")

# BigQuery tables
PLAYER_SEASON_AVERAGES_TABLE = "nba_live.player_season_averages"
TEAM_SEASON_AVERAGES_TABLE = "nba_live.team_season_averages"

# Rate limiting
REQUEST_DELAY_SEC = 0.3  # Polite throttling between requests
BATCH_SIZE = 100  # API per_page limit

# ======================================================
# Category/Type Mappings (from API docs)
# ======================================================

# Player season averages category/type combinations
PLAYER_CATEGORY_TYPES: Dict[str, List[Optional[str]]] = {
    "general": ["base", "advanced", "usage", "scoring", "defense", "misc"],
    "clutch": ["advanced", "base", "misc", "scoring", "usage"],
    "defense": ["2_pointers", "3_pointers", "greater_than_15ft", "less_than_10ft", "less_than_6ft", "overall"],
    "shooting": ["5ft_range", "by_zone"],
    "playtype": ["cut", "handoff", "isolation", "offrebound", "offscreen", "postup", "prballhandler", "prrollman", "spotup", "transition", "misc"],
    "tracking": ["painttouch", "efficiency", "speeddistance", "defense", "elbowtouch", "posttouch", "passing", "drives", "rebounding", "catchshoot", "pullupshot", "possessions"],
    "hustle": [None],  # No type required
    "shotdashboard": ["overall", "pullups", "catch_and_shoot", "less_than_10_ft"],
}

# Team season averages category/type combinations
TEAM_CATEGORY_TYPES: Dict[str, List[Optional[str]]] = {
    "general": ["base", "advanced", "scoring", "misc", "opponent", "defense", "violations"],
    "clutch": ["base", "advanced", "misc", "scoring"],
    "shooting": ["by_zone_base", "by_zone_opponent", "5ft_range_base", "5ft_range_opponent"],
    "playtype": ["cut", "handoff", "isolation", "offrebound", "offscreen", "postup", "prballhandler", "prrollman", "spotup", "transition", "misc"],
    "tracking": ["painttouch", "efficiency", "speeddistance", "defense", "elbowtouch", "posttouch", "passing", "drives", "rebounding", "catchshoot", "pullupshot", "possessions"],
    "hustle": [None],  # No type required
    "shotdashboard": ["overall", "pullups", "catch_and_shoot", "less_than_10_ft"],
}


# ======================================================
# BigQuery Client
# ======================================================

def get_bq_client() -> bigquery.Client:
    """Get BigQuery client with project from environment."""
    project = os.environ.get("GCP_PROJECT") or os.environ.get("GOOGLE_CLOUD_PROJECT")
    if project:
        return bigquery.Client(project=project)
    return bigquery.Client()


# ======================================================
# BallDontLie API
# ======================================================

def get_bdl_headers() -> Dict[str, str]:
    """Get BallDontLie API headers with auth."""
    api_key = os.environ.get("BALLDONTLIE_API_KEY")
    if not api_key:
        raise RuntimeError("BALLDONTLIE_API_KEY is missing")
    return {
        "Authorization": api_key,
        "Accept": "application/json",
    }


def fetch_player_season_averages(
    *,
    category: str,
    stat_type: Optional[str],
    season: int,
    season_type: str = "regular",
    player_ids: Optional[List[int]] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch player season averages for a category/type.

    Args:
        category: Category (general, clutch, defense, etc.)
        stat_type: Type within category (base, advanced, etc.) - None for hustle
        season: Season year (e.g., 2024 for 2024-2025)
        season_type: regular, playoffs, ist, playin
        player_ids: Optional list of player IDs to filter

    Returns:
        List of player season average records
    """
    import requests

    headers = get_bdl_headers()
    url = f"{BDL_BASE_V1}/season_averages/{category}"

    all_data: List[Dict[str, Any]] = []
    cursor: Optional[int] = None

    while True:
        params: Dict[str, Any] = {
            "season": season,
            "season_type": season_type,
            "per_page": BATCH_SIZE,
        }

        if stat_type:
            params["type"] = stat_type

        if player_ids:
            params["player_ids[]"] = player_ids

        if cursor:
            params["cursor"] = cursor

        resp = requests.get(url, params=params, headers=headers, timeout=30)

        # Handle invalid category/type combinations gracefully
        if resp.status_code == 400:
            print(f"[SEASON_AVG] Invalid combination: {category}/{stat_type}, skipping")
            return []

        resp.raise_for_status()

        result = resp.json()
        data = result.get("data", [])

        if not data:
            break

        all_data.extend(data)

        # Check for pagination
        meta = result.get("meta", {})
        next_cursor = meta.get("next_cursor")

        if not next_cursor:
            break

        cursor = next_cursor
        time.sleep(REQUEST_DELAY_SEC)

    return all_data


def fetch_team_season_averages(
    *,
    category: str,
    stat_type: Optional[str],
    season: int,
    season_type: str = "regular",
) -> List[Dict[str, Any]]:
    """
    Fetch team season averages for a category/type.

    Args:
        category: Category (general, clutch, shooting, etc.)
        stat_type: Type within category (base, advanced, etc.) - None for hustle
        season: Season year (e.g., 2024 for 2024-2025)
        season_type: regular, playoffs, ist, playin

    Returns:
        List of team season average records
    """
    import requests

    headers = get_bdl_headers()
    url = f"{BDL_BASE_V1}/team_season_averages/{category}"

    all_data: List[Dict[str, Any]] = []
    cursor: Optional[int] = None

    while True:
        params: Dict[str, Any] = {
            "season": season,
            "season_type": season_type,
            "per_page": BATCH_SIZE,
        }

        if stat_type:
            params["type"] = stat_type

        if cursor:
            params["cursor"] = cursor

        resp = requests.get(url, params=params, headers=headers, timeout=30)

        # Handle invalid category/type combinations gracefully
        if resp.status_code == 400:
            print(f"[TEAM_SEASON_AVG] Invalid combination: {category}/{stat_type}, skipping")
            return []

        resp.raise_for_status()

        result = resp.json()
        data = result.get("data", [])

        if not data:
            break

        all_data.extend(data)

        # Check for pagination (teams are <=30 but handle anyway)
        meta = result.get("meta", {})
        next_cursor = meta.get("next_cursor")

        if not next_cursor:
            break

        cursor = next_cursor
        time.sleep(REQUEST_DELAY_SEC)

    return all_data


# ======================================================
# Row Transformation
# ======================================================

def transform_player_record(
    record: Dict[str, Any],
    category: str,
    stat_type: Optional[str],
    run_ts: str,
) -> Dict[str, Any]:
    """Transform player season average record to BigQuery row."""
    player = record.get("player") or {}

    return {
        "run_ts": run_ts,
        "ingested_at": datetime.now(UTC_TZ).isoformat(),
        "season": record.get("season"),
        "season_type": record.get("season_type"),
        "category": category,
        "stat_type": stat_type,
        # Player info
        "player_id": player.get("id"),
        "player_first_name": player.get("first_name"),
        "player_last_name": player.get("last_name"),
        "player_position": player.get("position"),
        "player_height": player.get("height"),
        "player_weight": player.get("weight"),
        "player_jersey_number": player.get("jersey_number"),
        "player_college": player.get("college"),
        "player_country": player.get("country"),
        "player_draft_year": player.get("draft_year"),
        "player_draft_round": player.get("draft_round"),
        "player_draft_number": player.get("draft_number"),
        # Stats as JSON
        "stats": json.dumps(record.get("stats", {})),
    }


def transform_team_record(
    record: Dict[str, Any],
    category: str,
    stat_type: Optional[str],
    run_ts: str,
) -> Dict[str, Any]:
    """Transform team season average record to BigQuery row."""
    team = record.get("team") or {}

    return {
        "run_ts": run_ts,
        "ingested_at": datetime.now(UTC_TZ).isoformat(),
        "season": record.get("season"),
        "season_type": record.get("season_type"),
        "category": category,
        "stat_type": stat_type,
        # Team info
        "team_id": team.get("id"),
        "team_conference": team.get("conference"),
        "team_division": team.get("division"),
        "team_city": team.get("city"),
        "team_name": team.get("name"),
        "team_full_name": team.get("full_name"),
        "team_abbreviation": team.get("abbreviation"),
        # Stats as JSON
        "stats": json.dumps(record.get("stats", {})),
    }


# ======================================================
# BigQuery Insert
# ======================================================

def insert_rows_to_bq(rows: List[Dict[str, Any]], table: str) -> int:
    """Insert rows into BigQuery table."""
    if not rows:
        return 0

    client = get_bq_client()

    # Insert in batches of 500
    batch_size = 500
    total_inserted = 0

    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        errors = client.insert_rows_json(table, batch)

        if errors:
            print(f"[SEASON_AVG] BigQuery insert errors: {errors[:5]}")
            raise RuntimeError(f"BigQuery insert failed: {errors[:3]}")

        total_inserted += len(batch)

    return total_inserted


# ======================================================
# Public API Functions
# ======================================================

def ingest_player_season_averages(
    *,
    season: int,
    season_type: str = "regular",
    categories: Optional[List[str]] = None,
    table: str = PLAYER_SEASON_AVERAGES_TABLE,
) -> Dict[str, Any]:
    """
    Ingest player season averages for all category/type combinations.

    Args:
        season: Season year (e.g., 2024 for 2024-2025)
        season_type: regular, playoffs, ist, playin
        categories: Optional list of categories to ingest (defaults to all)
        table: Target BigQuery table

    Returns:
        Summary dict with counts
    """
    print(f"\n{'='*60}")
    print(f"[PLAYER_SEASON_AVG] INGESTING: season={season}, type={season_type}")
    print(f"{'='*60}")

    run_ts = datetime.now(UTC_TZ).isoformat()

    if categories is None:
        categories = list(PLAYER_CATEGORY_TYPES.keys())

    total_players = 0
    total_records = 0
    category_counts: Dict[str, int] = {}

    for category in categories:
        types = PLAYER_CATEGORY_TYPES.get(category, [])

        for stat_type in types:
            type_label = stat_type or "none"
            print(f"[PLAYER_SEASON_AVG] Fetching {category}/{type_label}...")

            records = fetch_player_season_averages(
                category=category,
                stat_type=stat_type,
                season=season,
                season_type=season_type,
            )

            if not records:
                print(f"[PLAYER_SEASON_AVG] No data for {category}/{type_label}")
                continue

            rows = [
                transform_player_record(r, category, stat_type, run_ts)
                for r in records
            ]

            inserted = insert_rows_to_bq(rows, table)

            key = f"{category}/{type_label}"
            category_counts[key] = inserted
            total_records += inserted

            unique_players = len(set(r.get("player_id") for r in rows if r.get("player_id")))
            total_players = max(total_players, unique_players)

            print(f"[PLAYER_SEASON_AVG] {category}/{type_label}: {inserted} records ({unique_players} players)")

            time.sleep(REQUEST_DELAY_SEC)

    print(f"\n{'='*60}")
    print(f"[PLAYER_SEASON_AVG] COMPLETE: {total_records} total records")
    print(f"{'='*60}\n")

    return {
        "season": season,
        "season_type": season_type,
        "total_records": total_records,
        "players": total_players,
        "category_counts": category_counts,
        "status": "ok",
    }


def ingest_team_season_averages(
    *,
    season: int,
    season_type: str = "regular",
    categories: Optional[List[str]] = None,
    table: str = TEAM_SEASON_AVERAGES_TABLE,
) -> Dict[str, Any]:
    """
    Ingest team season averages for all category/type combinations.

    Args:
        season: Season year (e.g., 2024 for 2024-2025)
        season_type: regular, playoffs, ist, playin
        categories: Optional list of categories to ingest (defaults to all)
        table: Target BigQuery table

    Returns:
        Summary dict with counts
    """
    print(f"\n{'='*60}")
    print(f"[TEAM_SEASON_AVG] INGESTING: season={season}, type={season_type}")
    print(f"{'='*60}")

    run_ts = datetime.now(UTC_TZ).isoformat()

    if categories is None:
        categories = list(TEAM_CATEGORY_TYPES.keys())

    total_teams = 0
    total_records = 0
    category_counts: Dict[str, int] = {}

    for category in categories:
        types = TEAM_CATEGORY_TYPES.get(category, [])

        for stat_type in types:
            type_label = stat_type or "none"
            print(f"[TEAM_SEASON_AVG] Fetching {category}/{type_label}...")

            records = fetch_team_season_averages(
                category=category,
                stat_type=stat_type,
                season=season,
                season_type=season_type,
            )

            if not records:
                print(f"[TEAM_SEASON_AVG] No data for {category}/{type_label}")
                continue

            rows = [
                transform_team_record(r, category, stat_type, run_ts)
                for r in records
            ]

            inserted = insert_rows_to_bq(rows, table)

            key = f"{category}/{type_label}"
            category_counts[key] = inserted
            total_records += inserted

            unique_teams = len(set(r.get("team_id") for r in rows if r.get("team_id")))
            total_teams = max(total_teams, unique_teams)

            print(f"[TEAM_SEASON_AVG] {category}/{type_label}: {inserted} records ({unique_teams} teams)")

            time.sleep(REQUEST_DELAY_SEC)

    print(f"\n{'='*60}")
    print(f"[TEAM_SEASON_AVG] COMPLETE: {total_records} total records")
    print(f"{'='*60}\n")

    return {
        "season": season,
        "season_type": season_type,
        "total_records": total_records,
        "teams": total_teams,
        "category_counts": category_counts,
        "status": "ok",
    }


def ingest_all_season_averages(
    *,
    season: int,
    season_type: str = "regular",
) -> Dict[str, Any]:
    """
    Ingest both player and team season averages.

    Args:
        season: Season year (e.g., 2024 for 2024-2025)
        season_type: regular, playoffs, ist, playin

    Returns:
        Summary dict with combined counts
    """
    print(f"\n{'='*60}")
    print(f"[SEASON_AVG] FULL INGEST: season={season}, type={season_type}")
    print(f"{'='*60}")

    player_result = ingest_player_season_averages(season=season, season_type=season_type)
    team_result = ingest_team_season_averages(season=season, season_type=season_type)

    return {
        "season": season,
        "season_type": season_type,
        "player": player_result,
        "team": team_result,
        "status": "ok",
    }


def get_current_season() -> int:
    """
    Get the current NBA season year.

    NBA season starts in October, so Oct-Dec = that year's season.
    """
    now = datetime.now(NBA_TZ)
    if now.month >= 10:
        return now.year
    return now.year - 1


def ingest_current_season(season_type: str = "regular") -> Dict[str, Any]:
    """
    Ingest season averages for the current NBA season.

    This is the primary function for daily scheduled ingestion.

    Returns:
        Summary dict with counts
    """
    season = get_current_season()
    return ingest_all_season_averages(season=season, season_type=season_type)


# ======================================================
# CLI Entry Point
# ======================================================

if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage:")
        print("  python ingest.py current [season_type]")
        print("  python ingest.py season <year> [season_type]")
        print("  python ingest.py player <year> [season_type]")
        print("  python ingest.py team <year> [season_type]")
        print("")
        print("Examples:")
        print("  python ingest.py current")
        print("  python ingest.py current playoffs")
        print("  python ingest.py season 2024")
        print("  python ingest.py player 2024 regular")
        print("  python ingest.py team 2024 playoffs")
        sys.exit(1)

    command = sys.argv[1]

    if command == "current":
        season_type = sys.argv[2] if len(sys.argv) >= 3 else "regular"
        result = ingest_current_season(season_type=season_type)
        print(f"Result: {json.dumps(result, indent=2)}")

    elif command == "season" and len(sys.argv) >= 3:
        season = int(sys.argv[2])
        season_type = sys.argv[3] if len(sys.argv) >= 4 else "regular"
        result = ingest_all_season_averages(season=season, season_type=season_type)
        print(f"Result: {json.dumps(result, indent=2)}")

    elif command == "player" and len(sys.argv) >= 3:
        season = int(sys.argv[2])
        season_type = sys.argv[3] if len(sys.argv) >= 4 else "regular"
        result = ingest_player_season_averages(season=season, season_type=season_type)
        print(f"Result: {json.dumps(result, indent=2)}")

    elif command == "team" and len(sys.argv) >= 3:
        season = int(sys.argv[2])
        season_type = sys.argv[3] if len(sys.argv) >= 4 else "regular"
        result = ingest_team_season_averages(season=season, season_type=season_type)
        print(f"Result: {json.dumps(result, indent=2)}")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)

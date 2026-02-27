"""
Modern MLS public API client (production-safe).

Confirmed working endpoints:

Season schedule:
  /matches/seasons/{season_id}
    ?match_date[gte]=YYYY-MM-DD
    &match_date[lte]=YYYY-MM-DD
    &per_page=100
    &page=N

Team season stats:
  /statistics/clubs/competitions/{competition_id}/seasons/{season_id}

Player season stats:
  sportapi.mlssoccer.com/api/stats/players/competition/{competition_id}/season/{season_id}
"""

from __future__ import annotations
import argparse
import json
import logging
import os
import time
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

STATS_API = "https://stats-api.mlssoccer.com"
SPORT_API = "https://sportapi.mlssoccer.com"

DEFAULT_COMPETITION_ID = "MLS-COM-000001"

DEFAULT_SEASON_ID_BY_YEAR = {
    2024: "MLS-SEA-0001K8",
    2025: "MLS-SEA-0001K9",
    2026: "MLS-SEA-0001KA",
}

TIMEOUT = 30
PAGE_SIZE = 100
RETRY_ATTEMPTS = 4

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://www.mlssoccer.com",
    "Referer": "https://www.mlssoccer.com/",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _competition_id() -> str:
    return os.getenv("MLSSOCCER_COMPETITION_ID_V2", DEFAULT_COMPETITION_ID)


def _season_id_for_year(season: int) -> str:
    if season in DEFAULT_SEASON_ID_BY_YEAR:
        return DEFAULT_SEASON_ID_BY_YEAR[season]
    raise RuntimeError(f"No season_id configured for {season}")


def _get(url: str, params: Optional[Dict[str, Any]] = None) -> Any:
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = requests.get(
                url,
                headers=HEADERS,
                params=params or {},
                timeout=TIMEOUT,
            )
        except requests.RequestException:
            if attempt == RETRY_ATTEMPTS:
                raise
            time.sleep(2 ** attempt)
            continue

        if resp.status_code < 400:
            return resp.json()

        if resp.status_code in (404,):
            # Safe skip for empty windows
            return {}

        if resp.status_code in (429, 500, 502, 503, 504):
            if attempt < RETRY_ATTEMPTS:
                time.sleep(2 ** attempt)
                continue

        raise RuntimeError(f"HTTP {resp.status_code} from {url}: {resp.text[:200]}")

    raise RuntimeError(f"Failed request to {url}")

def _extract_list(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return payload

    if isinstance(payload, dict):

        # direct known keys
        for key in (
            "matches",
            "content",
            "results",
            "schedule",
            "clubs",
            "players",
        ):
            if isinstance(payload.get(key), list):
                return payload[key]

        # data patterns
        data = payload.get("data")

        if isinstance(data, list):
            return data

        if isinstance(data, dict):
            for key in ("items", "matches", "clubs", "players"):
                if isinstance(data.get(key), list):
                    return data[key]

    return []
def _paginate(
    url: str,
    base_params: Dict[str, Any],
    page_param: str = "page",
    size_param: str = "per_page",
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    page = 1

    while True:
        params = dict(base_params)
        params[page_param] = page
        params[size_param] = PAGE_SIZE

        payload = _get(url, params)
        batch = _extract_list(payload)

        if not batch:
            break

        rows.extend(batch)

        if len(batch) < PAGE_SIZE:
            break

        page += 1

    return rows


# ---------------------------------------------------------------------------
# Schedule
# ---------------------------------------------------------------------------

def fetch_schedule(season: int) -> List[Dict[str, Any]]:
    season_id = _season_id_for_year(season)
    url = f"{STATS_API}/matches/seasons/{season_id}"

    rows: List[Dict[str, Any]] = []
    seen_ids = set()

    current = date(season, 1, 1)
    end = date(season, 12, 31)

    while current <= end:
        window_end = current + timedelta(days=6)

        params = {
            "match_date[gte]": current.isoformat(),
            "match_date[lte]": window_end.isoformat(),
            "sort": "planned_kickoff_time:asc",
        }

        payload = _get(url, params)
        batch = _extract_list(payload)

        for match in batch:
            comp_name = match.get("competition_name", "")

            # Keep only MLS first division
            if comp_name != "Major League Soccer - Regular Season":
                continue

            match_id = match.get("match_id") or match.get("id")
            if not match_id:
                continue

            if match_id in seen_ids:
                continue

            seen_ids.add(match_id)
            rows.append(match)

        current += timedelta(days=7)

    logger.info("schedule: %d matches", len(rows))
    return rows


# ---------------------------------------------------------------------------
# Team Stats
# ---------------------------------------------------------------------------

def fetch_team_stats(season: int) -> List[Dict[str, Any]]:
    season_id = _season_id_for_year(season)

    url = (
        f"{STATS_API}/statistics/clubs/competitions/"
        f"{_competition_id()}/seasons/{season_id}"
    )

    payload = _get(url)

    rows = payload.get("team_statistics", [])
    if not isinstance(rows, list):
        rows = []

    logger.info("team_stats: %d clubs", len(rows))
    return rows
# ---------------------------------------------------------------------------
# Player Stats
# ---------------------------------------------------------------------------

def fetch_player_stats(season: int) -> List[Dict[str, Any]]:
    season_id = _season_id_for_year(season)

    teams = fetch_team_stats(season)

    all_players = []

    for team in teams:
        team_id = team["team_id"]

        url = (
            f"{STATS_API}/statistics/players/competitions/"
            f"{_competition_id()}/seasons/{season_id}/clubs/{team_id}"
        )

        payload = _get(url)
        print("TEAM:", team_id)
        print(json.dumps(payload, indent=2)[:500])
        batch = payload.get("player_statistics", [])

        all_players.extend(batch)
        time.sleep(0.2)

    # dedupe
    unique = {p["player_id"]: p for p in all_players}
    rows = list(unique.values())

    logger.info("player_stats: %d players", len(rows))
    return rows


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Fetch MLS data")
    p.add_argument("--season", type=int, required=True)
    p.add_argument("--json", action="store_true")
    return p


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    args = _build_parser().parse_args()

    result = {
        "season": args.season,
        "schedule": fetch_schedule(args.season),
        "team_stats": fetch_team_stats(args.season),
        "player_stats": fetch_player_stats(args.season),
    }

    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print({k: len(v) if isinstance(v, list) else v for k, v in result.items()})

def fetch_team_game_stats(season: int) -> List[Dict[str, Any]]:
    logger.warning("team_game_stats not implemented yet — returning []")
    return []


def fetch_player_game_stats(season: int) -> List[Dict[str, Any]]:
    logger.warning("player_game_stats not implemented yet — returning []")
    return []

if __name__ == "__main__":
    main()
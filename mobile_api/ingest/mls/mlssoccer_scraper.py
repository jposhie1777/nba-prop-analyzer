"""
HTTP client for the stats-api.mlssoccer.com public stats API.

Fetches three data sets for a given MLS season:
  - schedule  : full match schedule (past + upcoming)
  - team_stats : aggregated per-club stats (goals, shots, possession, etc.)
  - player_stats: aggregated per-player stats (goals, assists, minutes, etc.)

All data is returned as plain lists-of-dicts so callers can persist however they
wish.  The module can also be run directly:

  python -m mobile_api.ingest.mls.mlssoccer_scraper --season 2025
  python -m mobile_api.ingest.mls.mlssoccer_scraper --season 2025 --json
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE_URL = os.getenv(
    "MLSSOCCER_STATS_BASE_URL",
    "https://stats-api.mlssoccer.com/v1",
)
COMPETITION_OPTA_ID = os.getenv("MLSSOCCER_COMPETITION_ID", "MLS")
TIMEOUT = int(os.getenv("MLSSOCCER_TIMEOUT_SECONDS", "30"))
PAGE_SIZE = int(os.getenv("MLSSOCCER_PAGE_SIZE", "100"))
RETRY_ATTEMPTS = int(os.getenv("MLSSOCCER_RETRY_ATTEMPTS", "4"))
RETRY_BASE_SECONDS = float(os.getenv("MLSSOCCER_RETRY_BASE_SECONDS", "2.0"))
RETRY_CAP_SECONDS = float(os.getenv("MLSSOCCER_RETRY_CAP_SECONDS", "30.0"))

# The MLS website frontend sets these; mirroring them keeps us from getting
# rejected as a headless bot.
_DEFAULT_HEADERS: Dict[str, str] = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://www.mlssoccer.com",
    "Referer": "https://www.mlssoccer.com/",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}


class MlsSoccerApiError(RuntimeError):
    pass


# ---------------------------------------------------------------------------
# Low-level HTTP helpers
# ---------------------------------------------------------------------------

def _get(path: str, params: Optional[Dict[str, Any]] = None) -> Any:
    """GET a path on the stats API with retry logic."""
    url = f"{BASE_URL}/{path.lstrip('/')}"
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = requests.get(
                url,
                headers=_DEFAULT_HEADERS,
                params=params or {},
                timeout=TIMEOUT,
            )
        except requests.RequestException as exc:
            if attempt < RETRY_ATTEMPTS:
                delay = min(RETRY_BASE_SECONDS * (2 ** (attempt - 1)), RETRY_CAP_SECONDS)
                logger.warning("Request error %s – retry %d in %.1fs", exc, attempt, delay)
                time.sleep(delay)
                continue
            raise MlsSoccerApiError(f"Request failed after {RETRY_ATTEMPTS} attempts: {exc}") from exc

        if resp.status_code < 400:
            return resp.json()

        if resp.status_code in (429, 500, 502, 503, 504) and attempt < RETRY_ATTEMPTS:
            delay = min(RETRY_BASE_SECONDS * (2 ** (attempt - 1)), RETRY_CAP_SECONDS)
            logger.warning("HTTP %d – retry %d in %.1fs", resp.status_code, attempt, delay)
            time.sleep(delay)
            continue

        raise MlsSoccerApiError(
            f"HTTP {resp.status_code} from {url}: {resp.text[:200]}"
        )

    raise MlsSoccerApiError(f"Exhausted {RETRY_ATTEMPTS} retries for {url}")


def _fetch_paginated(path: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Iterate offset-based pages until no more data is returned."""
    rows: List[Dict[str, Any]] = []
    params = dict(params)
    params.setdefault("limit", PAGE_SIZE)
    offset = 0

    while True:
        params["offset"] = offset
        payload = _get(path, params)

        # The API can return a plain list or a {"data": [...]} envelope.
        if isinstance(payload, list):
            batch = payload
        elif isinstance(payload, dict):
            batch = payload.get("data") or payload.get("content") or []
            if not isinstance(batch, list):
                batch = [payload] if payload else []
        else:
            batch = []

        if not batch:
            break

        rows.extend(batch)

        # If the response was paged and returned fewer rows than the page size,
        # we've reached the last page.
        if len(batch) < params["limit"]:
            break

        offset += len(batch)

    return rows


# ---------------------------------------------------------------------------
# Public fetch functions
# ---------------------------------------------------------------------------

def fetch_schedule(season: int) -> List[Dict[str, Any]]:
    """
    Fetch the full match schedule for *season*.

    Returns a list of match dicts as provided by stats-api.mlssoccer.com.
    Each entry includes: matchday, date, home_club, away_club, score, status,
    venue, etc.
    """
    params: Dict[str, Any] = {
        "competition_opta_id": COMPETITION_OPTA_ID,
        "season_opta_id": season,
        "order_by": "match_date",
    }
    return _fetch_paginated("schedule", params)


def fetch_team_stats(season: int) -> List[Dict[str, Any]]:
    """
    Fetch per-club aggregated season statistics for *season*.

    Returns a list of club-stat dicts:
    goals_scored, goals_conceded, assists, shots, shots_on_target,
    possession_percentage, pass_completion, etc.
    """
    params: Dict[str, Any] = {
        "competition_opta_id": COMPETITION_OPTA_ID,
        "season_opta_id": season,
        "order_by": "club_short_name",
    }
    return _fetch_paginated("clubs", params)


def fetch_player_stats(season: int) -> List[Dict[str, Any]]:
    """
    Fetch per-player aggregated season statistics for *season*.

    Returns a list of player-stat dicts:
    player_name, club, position, appearances, goals, assists,
    minutes_played, yellow_cards, red_cards, shots, etc.
    """
    params: Dict[str, Any] = {
        "competition_opta_id": COMPETITION_OPTA_ID,
        "season_opta_id": season,
        "order_by": "player_last_name",
    }
    return _fetch_paginated("players", params)


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Fetch MLS data from stats-api.mlssoccer.com"
    )
    p.add_argument("--season", type=int, required=True, help="MLS season year, e.g. 2025")
    p.add_argument(
        "--data",
        choices=["schedule", "team_stats", "player_stats", "all"],
        default="all",
        help="Which data set to fetch (default: all)",
    )
    p.add_argument("--json", action="store_true", dest="as_json", help="Pretty-print JSON output")
    return p


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = _build_parser().parse_args()

    results: Dict[str, Any] = {"season": args.season}

    if args.data in ("schedule", "all"):
        schedule = fetch_schedule(args.season)
        results["schedule"] = schedule
        logger.info("schedule: %d matches", len(schedule))

    if args.data in ("team_stats", "all"):
        team_stats = fetch_team_stats(args.season)
        results["team_stats"] = team_stats
        logger.info("team_stats: %d clubs", len(team_stats))

    if args.data in ("player_stats", "all"):
        player_stats = fetch_player_stats(args.season)
        results["player_stats"] = player_stats
        logger.info("player_stats: %d players", len(player_stats))

    if args.as_json:
        print(json.dumps(results, indent=2, default=str))
    else:
        for key, value in results.items():
            if isinstance(value, list):
                print(f"{key}: {len(value)} records")


if __name__ == "__main__":
    main()

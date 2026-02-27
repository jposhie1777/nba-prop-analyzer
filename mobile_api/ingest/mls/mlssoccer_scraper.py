"""
HTTP client for the stats-api.mlssoccer.com public stats API.

Fetches five data sets for a given MLS season:
  - schedule         : full match schedule (past + upcoming)
  - team_stats       : aggregated per-club season stats (goals, shots, possession, etc.)
  - player_stats     : aggregated per-player season stats (goals, assists, minutes, etc.)
  - team_game_stats  : per-club per-match stats (one row per club per game)
  - player_game_stats: per-player per-match stats (one row per player per game)

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
from dataclasses import dataclass
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
DEFAULT_BASE_URLS = [
    BASE_URL,
    "https://stats-api.mlssoccer.com",
    "https://stats-api.mlssoccer.com/v2",
]
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


@dataclass
class MlsSoccerApiError(RuntimeError):
    message: str
    status_code: Optional[int] = None
    url: Optional[str] = None
    response_body: Optional[str] = None

    def __str__(self) -> str:
        return self.message


# ---------------------------------------------------------------------------
# Low-level HTTP helpers
# ---------------------------------------------------------------------------

def _iter_base_urls() -> List[str]:
    configured = os.getenv("MLSSOCCER_STATS_BASE_URLS", "")
    if configured.strip():
        candidates = [item.strip() for item in configured.split(",") if item.strip()]
    else:
        candidates = list(DEFAULT_BASE_URLS)

    # Keep order, remove duplicates.
    deduped: List[str] = []
    seen = set()
    for url in candidates:
        normalized = url.rstrip("/")
        if normalized and normalized not in seen:
            seen.add(normalized)
            deduped.append(normalized)
    return deduped


def _get(path: str, params: Optional[Dict[str, Any]] = None, base_url: Optional[str] = None) -> Any:
    """GET a path on the stats API with retry logic."""
    root = (base_url or BASE_URL).rstrip("/")
    url = f"{root}/{path.lstrip('/')}"
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
            raise MlsSoccerApiError(
                f"Request failed after {RETRY_ATTEMPTS} attempts: {exc}",
                url=url,
            ) from exc

        if resp.status_code < 400:
            return resp.json()

        if resp.status_code in (429, 500, 502, 503, 504) and attempt < RETRY_ATTEMPTS:
            delay = min(RETRY_BASE_SECONDS * (2 ** (attempt - 1)), RETRY_CAP_SECONDS)
            logger.warning("HTTP %d – retry %d in %.1fs", resp.status_code, attempt, delay)
            time.sleep(delay)
            continue

        raise MlsSoccerApiError(
            f"HTTP {resp.status_code} from {url}: {resp.text[:200]}",
            status_code=resp.status_code,
            url=url,
            response_body=resp.text[:1000],
        )

    raise MlsSoccerApiError(f"Exhausted {RETRY_ATTEMPTS} retries for {url}", url=url)


def _fetch_paginated(path: str, params: Dict[str, Any], base_url: Optional[str] = None) -> List[Dict[str, Any]]:
    """Iterate offset-based pages until no more data is returned."""
    rows: List[Dict[str, Any]] = []
    params = dict(params)
    params.setdefault("limit", PAGE_SIZE)
    offset = 0

    while True:
        params["offset"] = offset
        payload = _get(path, params, base_url=base_url)

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


def _fetch_paginated_with_fallback(paths: List[str], params: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Try endpoint path candidates across one or more base URLs until one succeeds.

    MLS has changed endpoint names and URL versions over time. If an endpoint
    returns 404, we automatically try the next candidate path and/or base URL
    so ingest jobs can keep running without a code deploy.
    """
    if not paths:
        raise ValueError("paths must include at least one endpoint")

    base_urls = _iter_base_urls()
    if not base_urls:
        raise ValueError("No MLS base URLs configured")

    last_error: Optional[MlsSoccerApiError] = None
    for base_index, base_url in enumerate(base_urls):
        for path_index, path in enumerate(paths):
            if base_index > 0 or path_index > 0:
                logger.info("Trying MLS endpoint base='%s' path='%s'", base_url, path)
            try:
                return _fetch_paginated(path, params, base_url=base_url)
            except MlsSoccerApiError as exc:
                last_error = exc
                if exc.status_code == 404:
                    continue
                raise

    if last_error:
        raise last_error
    raise MlsSoccerApiError("No endpoint paths were attempted")


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
    path_candidates = [
        os.getenv("MLSSOCCER_SCHEDULE_PATH", "schedule"),
        os.getenv("MLSSOCCER_SCHEDULE_FALLBACK_PATH", "schedules"),
    ]
    return _fetch_paginated_with_fallback(path_candidates, params)


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
    path_candidates = [
        os.getenv("MLSSOCCER_TEAM_STATS_PATH", "clubs"),
        os.getenv("MLSSOCCER_TEAM_STATS_FALLBACK_PATH", "teams"),
    ]
    return _fetch_paginated_with_fallback(path_candidates, params)


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
    path_candidates = [
        os.getenv("MLSSOCCER_PLAYER_STATS_PATH", "players"),
        os.getenv("MLSSOCCER_PLAYER_STATS_FALLBACK_PATH", "athletes"),
    ]
    return _fetch_paginated_with_fallback(path_candidates, params)


def fetch_team_game_stats(season: int) -> List[Dict[str, Any]]:
    """
    Fetch per-club per-match statistics for *season*.

    Targets the /v1/stats/clubs endpoint which returns one row per club per
    completed match, including: match_id, club_id, possession_percentage,
    shots, shots_on_target, passes, pass_completion, fouls, corners, offsides,
    goals_scored, goals_conceded, etc.
    """
    params: Dict[str, Any] = {
        "competition_opta_id": COMPETITION_OPTA_ID,
        "season_opta_id": season,
        "order_by": "match_date",
    }
    path_candidates = [
        os.getenv("MLSSOCCER_TEAM_GAME_STATS_PATH", "stats/clubs"),
        os.getenv("MLSSOCCER_TEAM_GAME_STATS_FALLBACK_PATH", "match-stats/clubs"),
    ]
    return _fetch_paginated_with_fallback(path_candidates, params)


def fetch_player_game_stats(season: int) -> List[Dict[str, Any]]:
    """
    Fetch per-player per-match statistics for *season*.

    Targets the /v1/stats/players endpoint which returns one row per player per
    completed match, including: match_id, player_id, club_id, position,
    minutes_played, goals, assists, shots, shots_on_target, passes,
    key_passes, yellow_cards, red_cards, tackles, interceptions, etc.
    """
    params: Dict[str, Any] = {
        "competition_opta_id": COMPETITION_OPTA_ID,
        "season_opta_id": season,
        "order_by": "match_date",
    }
    path_candidates = [
        os.getenv("MLSSOCCER_PLAYER_GAME_STATS_PATH", "stats/players"),
        os.getenv("MLSSOCCER_PLAYER_GAME_STATS_FALLBACK_PATH", "match-stats/players"),
    ]
    return _fetch_paginated_with_fallback(path_candidates, params)


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
        choices=[
            "schedule",
            "team_stats",
            "player_stats",
            "team_game_stats",
            "player_game_stats",
            "all",
        ],
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

    if args.data in ("team_game_stats", "all"):
        team_game_stats = fetch_team_game_stats(args.season)
        results["team_game_stats"] = team_game_stats
        logger.info("team_game_stats: %d rows", len(team_game_stats))

    if args.data in ("player_game_stats", "all"):
        player_game_stats = fetch_player_game_stats(args.season)
        results["player_game_stats"] = player_game_stats
        logger.info("player_game_stats: %d rows", len(player_game_stats))

    if args.as_json:
        print(json.dumps(results, indent=2, default=str))
    else:
        for key, value in results.items():
            if isinstance(value, list):
                print(f"{key}: {len(value)} records")


if __name__ == "__main__":
    main()

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
  /statistics/players/competitions/{competition_id}/seasons/{season_id}

Per-match club stats (one row per club per completed match):
  Fetched by iterating completed matches from the schedule and filtering
  the season stats endpoint by match_opta_id / match_id.

Per-match player stats (one row per player per completed match):
  Same iteration strategy as per-match club stats.
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
RETRY_ATTEMPTS = 6

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
# Persistent HTTP Session (prevents TLS handshake stalls in cloud envs)
# ---------------------------------------------------------------------------

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

_session = requests.Session()
_session.headers.update(HEADERS)

_retry = Retry(
    total=4,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
)

_adapter = HTTPAdapter(
    max_retries=_retry,
    pool_connections=20,
    pool_maxsize=20,
)

_session.mount("https://", _adapter)
_session.mount("http://", _adapter)


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
            resp = _session.get(
                url,
                params=params or {},
                timeout=(5, 8)   # 8 second read timeout
            )
        except requests.RequestException as exc:
            if attempt == RETRY_ATTEMPTS:
                raise RuntimeError(f"Request failed after retries: {url}") from exc
            time.sleep(2 ** attempt)
            continue

        if resp.status_code < 400:
            try:
                return resp.json()
            except ValueError:
                raise RuntimeError(f"Invalid JSON from {url}")

        if resp.status_code == 404:
            return {}

        if resp.status_code in (429, 500, 502, 503, 504):
            if attempt < RETRY_ATTEMPTS:
                time.sleep(2 ** attempt)
                continue

        raise RuntimeError(
            f"HTTP {resp.status_code} from {url}: {resp.text[:200]}"
        )

    raise RuntimeError(f"Failed request to {url}")

def _extract_list(payload: Any) -> List[Dict[str, Any]]:
    """Extract a row list from any envelope shape the MLS API returns."""
    if isinstance(payload, list):
        return payload

    if isinstance(payload, dict):
        for key in (
            "matches",
            "content",
            "results",
            "schedule",
            "clubs",
            "players",
            "teams",
            "statistics",
            "stats",
            "items",
            "team_statistics",
            "player_statistics",
            "match_statistics",
        ):
            if isinstance(payload.get(key), list):
                return payload[key]

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
    MAX_PAGES = 50

    while page <= MAX_PAGES:
        params = dict(base_params)
        params[page_param] = page
        params[size_param] = PAGE_SIZE

        logger.info("Paginating %s page=%s", url, page)

        payload = _get(url, params)
        batch = _extract_list(payload)

        if not batch:
            break

        rows.extend(batch)

        if len(batch) < PAGE_SIZE:
            break

        page += 1

    if page > MAX_PAGES:
        logger.warning("Max pagination limit reached for %s", url)

    return rows

# ---------------------------------------------------------------------------
# Schedule
# ---------------------------------------------------------------------------

def fetch_schedule(season: int) -> List[Dict[str, Any]]:
    season_id = _season_id_for_year(season)
    url = f"{STATS_API}/matches/seasons/{season_id}"

    rows: List[Dict[str, Any]] = []
    seen_ids: set = set()

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
# Team Stats (season aggregate)
# ---------------------------------------------------------------------------

def fetch_team_stats(season: int) -> List[Dict[str, Any]]:
    season_id = _season_id_for_year(season)

    url = (
        f"{STATS_API}/statistics/clubs/competitions/"
        f"{_competition_id()}/seasons/{season_id}"
    )

    payload = _get(url)

    rows = payload.get("team_statistics") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        rows = _extract_list(payload)

    logger.info("team_stats: %d clubs", len(rows))
    return rows


# ---------------------------------------------------------------------------
# Player Stats (season aggregate)
# ---------------------------------------------------------------------------

def fetch_player_stats(season: int) -> List[Dict[str, Any]]:
    """
    Fetch per-player aggregated season statistics for *season*.

    Returns a list of player-stat dicts:
    player_name, club, position, appearances, goals, assists,
    minutes_played, yellow_cards, red_cards, shots, etc.
    """
    season_id = _season_id_for_year(season)
    comp_id = _competition_id()

    url = (
        os.getenv("MLSSOCCER_PLAYER_STATS_URL")
        or f"{STATS_API}/statistics/players/competitions/{comp_id}/seasons/{season_id}"
    )

    logger.info("Fetching player_stats from %s", url)
    payload = _get(url)
    rows = _extract_list(payload)

    if not rows:
        logger.warning("Primary player stats endpoint returned 0 rows — trying fallback")

    if not rows:
        # Fallback: try sportapi endpoint
        fallback = f"{SPORT_API}/api/stats/players/competition/{comp_id}/season/{season_id}"
        rows = _paginate(fallback, {}, page_param="page", size_param="pageSize")

    logger.info("player_stats: %d players", len(rows))
    return rows


# ---------------------------------------------------------------------------
# Per-match helpers
# ---------------------------------------------------------------------------

_COMPLETED_STATUSES = frozenset(
    {"final", "ft", "full time", "completed", "played", "finished", "post", "complete"}
)


def _is_match_completed(match: Dict[str, Any]) -> bool:
    """
    Return True when the match result is finalized.
    """

    status = (
        match.get("match_status")
        or match.get("status")
        or ""
    ).lower()

    # MLS uses "finalWhistle"
    if status in {
        "finalwhistle",
        "final",
        "ft",
        "completed",
        "finished",
    }:
        return True

    # Fallback: check goals fields MLS actually provides
    home_goals = match.get("home_team_goals")
    away_goals = match.get("away_team_goals")

    if home_goals is not None and away_goals is not None:
        return True

    return False

def _fetch_stats_for_match(
    base_url: str,
    match_id: str,
) -> List[Dict[str, Any]]:
    """
    Fetch statistics rows for one match from *base_url*.

    Tries several match-filter query-param names in order
    (match_opta_id → match_id → matchId).  Returns [] when the
    match has no published stats yet (upcoming or data not available).
    """
    for filter_key in ("match_opta_id", "match_id", "matchId"):
        payload = _get(base_url, {filter_key: match_id, "per_page": PAGE_SIZE})
        rows = _extract_list(payload)
        if rows:
            return rows
    return []


# ---------------------------------------------------------------------------
# Team game stats (per-club per-match)
# ---------------------------------------------------------------------------

def fetch_team_game_stats(season: int) -> List[Dict[str, Any]]:
    """
    Fetch per-club per-match statistics for *season*.

    Iterates every completed match from the season schedule and requests
    club statistics filtered to that match.  Each row is augmented with
    ``match_id`` and ``match_date`` so downstream consumers don't need
    to re-join against the schedule.
    """
    season_id = _season_id_for_year(season)
    comp_id = _competition_id()

    matches = fetch_schedule(season)

    # DEBUG: inspect first match structure
    if matches:
        print("DEBUG SAMPLE MATCH:")

    completed = [m for m in matches if _is_match_completed(m)]
    logger.info("team_game_stats: %d completed matches to process", len(completed))

    base_url = (
        os.getenv("MLSSOCCER_TEAM_GAME_STATS_URL")
        or f"{STATS_API}/statistics/clubs/competitions/{comp_id}/seasons/{season_id}"
    )

    all_rows: List[Dict[str, Any]] = []
    for match in completed:
        match_id = (
            match.get("match_id")
            or match.get("id")
            or match.get("matchId")
            or match.get("opta_id")
            or ""
        )
        if not match_id:
            continue

        rows = _fetch_stats_for_match(base_url, match_id)
        match_date = (
            match.get("match_date")
            or match.get("date")
            or match.get("matchDate")
            or ""
        )
        for row in rows:
            row.setdefault("match_id", match_id)
            row.setdefault("match_date", match_date)
        all_rows.extend(rows)
        time.sleep(0.05)  # polite rate-limiting between match requests

    logger.info("team_game_stats: %d rows total", len(all_rows))
    return all_rows


# ---------------------------------------------------------------------------
# Player game stats (per-player per-match)
# ---------------------------------------------------------------------------

def fetch_player_game_stats(season: int) -> List[Dict[str, Any]]:
    """
    Fetch per-player per-match statistics for *season*.

    Iterates every completed match from the season schedule and requests
    player statistics filtered to that match.  Each row is augmented with
    ``match_id`` and ``match_date``.
    """
    season_id = _season_id_for_year(season)
    comp_id = _competition_id()

    matches = fetch_schedule(season)
    completed = [m for m in matches if _is_match_completed(m)]
    logger.info("player_game_stats: %d completed matches to process", len(completed))

    base_url = (
        os.getenv("MLSSOCCER_PLAYER_GAME_STATS_URL")
        or f"{STATS_API}/statistics/players/competitions/{comp_id}/seasons/{season_id}"
    )

    all_rows: List[Dict[str, Any]] = []
    for match in completed:
        match_id = (
            match.get("match_id")
            or match.get("id")
            or match.get("matchId")
            or match.get("opta_id")
            or ""
        )
        if not match_id:
            continue

        rows = _fetch_stats_for_match(base_url, match_id)
        match_date = (
            match.get("match_date")
            or match.get("date")
            or match.get("matchDate")
            or ""
        )
        for row in rows:
            row.setdefault("match_id", match_id)
            row.setdefault("match_date", match_date)
        all_rows.extend(rows)
        time.sleep(0.05)  # polite rate-limiting between match requests

    logger.info("player_game_stats: %d rows total", len(all_rows))
    return all_rows


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Fetch MLS data")
    p.add_argument("--season", type=int, required=True)
    p.add_argument(
        "--data",
        choices=["schedule", "team_stats", "player_stats", "team_game_stats", "player_game_stats", "all"],
        default="all",
    )
    p.add_argument("--json", action="store_true")
    return p


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    args = _build_parser().parse_args()

    result: Dict[str, Any] = {"season": args.season}

    if args.data in ("schedule", "all"):
        result["schedule"] = fetch_schedule(args.season)
    if args.data in ("team_stats", "all"):
        result["team_stats"] = fetch_team_stats(args.season)
    if args.data in ("player_stats", "all"):
        result["player_stats"] = fetch_player_stats(args.season)
    if args.data in ("team_game_stats", "all"):
        result["team_game_stats"] = fetch_team_game_stats(args.season)
    if args.data in ("player_game_stats", "all"):
        result["player_game_stats"] = fetch_player_game_stats(args.season)

    if args.json:
        print(json.dumps(result, indent=2, default=str))
    else:
        print({k: len(v) if isinstance(v, list) else v for k, v in result.items()})


if __name__ == "__main__":
    main()

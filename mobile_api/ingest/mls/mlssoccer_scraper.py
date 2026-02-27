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
    """
    Fetch per-player aggregated season statistics for *season*.

    Returns a list of player-stat dicts:
    player_name, club, position, appearances, goals, assists,
    minutes_played, yellow_cards, red_cards, shots, etc.
    """
    params: Dict[str, Any] = {
        "per_page": PAGE_SIZE,
    }
    path_candidates = [
        os.getenv(
            "MLSSOCCER_PLAYER_STATS_PATH",
            "statistics/players/competitions/{competition_id}/seasons/{season_id}",
        ),
        os.getenv("MLSSOCCER_PLAYER_STATS_FALLBACK_PATH", "players/seasons/{season_id}"),
        os.getenv("MLSSOCCER_PLAYER_STATS_FALLBACK_PATH_2", "players"),
    ]
    return _fetch_paginated_with_fallback(path_candidates, params, season=season)

    teams = fetch_team_stats(season)

# ---------------------------------------------------------------------------
# Per-match helpers (used by team_game_stats and player_game_stats)
# ---------------------------------------------------------------------------

_COMPLETED_STATUSES = frozenset(
    {"final", "ft", "full time", "completed", "played", "finished", "post", "complete"}
)


def _is_match_completed(match: Dict[str, Any]) -> bool:
    """Return True when the match result is decided."""
    status = (
        match.get("status")
        or match.get("match_status")
        or match.get("matchStatus")
        or ""
    ).lower()
    if status in _COMPLETED_STATUSES:
        return True
    # Fallback: both scores present
    home = (
        match.get("home_score")
        or match.get("score_home")
        or match.get("homeScore")
    )
    away = (
        match.get("away_score")
        or match.get("score_away")
        or match.get("awayScore")
    )
    return home is not None and away is not None


def _get_rows(payload: Any) -> List[Dict[str, Any]]:
    """
    Extract a list of row-dicts from any API response shape the MLS API returns.
    Handles plain lists, ``{"data": [...]}`` envelopes, and other common keys.
    """
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in (
            "data", "content", "matches", "items", "results",
            "statistics", "clubs", "players", "teams", "stats",
        ):
            val = payload.get(key)
            if isinstance(val, list):
                return val
        # Single-object envelope
        return [payload] if payload else []
    return []


def _fetch_stats_for_match(
    base_path: str,
    match_id: str,
    extra_params: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch statistics rows for a single match from *base_path*.

    Tries the query-param filter approach first (``match_opta_id``, then
    ``match_id``) and falls back to a match-scoped sub-path.  Returns an
    empty list when the match has no published stats yet (e.g. upcoming).
    """
    params: Dict[str, Any] = {"per_page": PAGE_SIZE, "page": 1}
    if extra_params:
        params.update(extra_params)

    for filter_key in ("match_opta_id", "match_id", "matchId"):
        try:
            payload = _get(base_path, {**params, filter_key: match_id})
            rows = _get_rows(payload)
            if rows:
                return rows
        except MlsSoccerApiError:
            pass

    # Last resort: try a match-scoped sub-path, e.g. matches/{id}/statistics/clubs
    segment = "clubs" if "clubs" in base_path else "players"
    try:
        payload = _get(f"matches/{match_id}/statistics/{segment}", {"per_page": PAGE_SIZE})
        return _get_rows(payload)
    except MlsSoccerApiError:
        pass

    return []


def fetch_team_game_stats(season: int) -> List[Dict[str, Any]]:
    """
    Fetch per-club per-match statistics for *season*.

    Iterates over every completed match returned by the season schedule and
    requests club statistics filtered to that match.  Each row is augmented
    with ``match_id`` and ``match_date`` so downstream consumers don't need
    to re-join.
    """
    season_id = _season_id_for_year(season)
    if not season_id:
        logger.warning(
            "No season_id configured for year %d – skipping team_game_stats", season
        )
        return []
    comp_id = _competition_id()

    matches = fetch_schedule(season)
    completed = [m for m in matches if _is_match_completed(m)]
    logger.info("team_game_stats: %d completed matches to process", len(completed))

    base_path = (
        os.getenv(
            "MLSSOCCER_TEAM_GAME_STATS_PATH",
            f"statistics/clubs/competitions/{comp_id}/seasons/{season_id}",
        )
    )

    all_rows: List[Dict[str, Any]] = []
    for match in completed:
        match_id = (
            match.get("id")
            or match.get("matchId")
            or match.get("opta_id")
            or ""
        )
        if not match_id:
            continue

        rows = _fetch_stats_for_match(base_path, match_id)
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

    return all_rows

    for team in teams:
        team_id = team["team_id"]

def fetch_player_game_stats(season: int) -> List[Dict[str, Any]]:
    """
    Fetch per-player per-match statistics for *season*.

    Iterates over every completed match returned by the season schedule and
    requests player statistics filtered to that match.  Each row is augmented
    with ``match_id`` and ``match_date``.
    """
    season_id = _season_id_for_year(season)
    if not season_id:
        logger.warning(
            "No season_id configured for year %d – skipping player_game_stats", season
        )
        return []
    comp_id = _competition_id()

    matches = fetch_schedule(season)
    completed = [m for m in matches if _is_match_completed(m)]
    logger.info("player_game_stats: %d completed matches to process", len(completed))

    base_path = (
        os.getenv(
            "MLSSOCCER_PLAYER_GAME_STATS_PATH",
            f"statistics/players/competitions/{comp_id}/seasons/{season_id}",
        )
    )

    all_rows: List[Dict[str, Any]] = []
    for match in completed:
        match_id = (
            match.get("id")
            or match.get("matchId")
            or match.get("opta_id")
            or ""
        )
        if not match_id:
            continue

        rows = _fetch_stats_for_match(base_path, match_id)
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

    return all_rows


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
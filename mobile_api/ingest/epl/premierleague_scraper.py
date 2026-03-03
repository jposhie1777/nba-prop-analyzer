from __future__ import annotations

import logging
import os
import time
from datetime import date
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

SDP_API_BASE = os.getenv(
    "PREMIERLEAGUE_SDP_API_BASE",
    "https://sdp-prem-prod.premier-league-prod.pulselive.com/api",
)
COMPETITION_ID = os.getenv("PREMIERLEAGUE_SDP_COMPETITION_ID", "8")
TIMEOUT = int(os.getenv("PREMIERLEAGUE_TIMEOUT_SECONDS", "30"))
PAGE_SIZE = int(os.getenv("PREMIERLEAGUE_PAGE_SIZE", "20"))
MAX_MATCHWEEK = int(os.getenv("PREMIERLEAGUE_MAX_MATCHWEEK", "60"))

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://www.premierleague.com",
    "Referer": "https://www.premierleague.com/",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}


class PremierLeagueApiError(RuntimeError):
    pass


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def _get(path: str, params: Optional[Dict[str, Any]] = None) -> Any:
    url = f"{SDP_API_BASE.rstrip('/')}/{path.lstrip('/')}"
    session = _session()
    for attempt in range(1, 6):
        try:
            response = session.get(url, params=params or {}, timeout=TIMEOUT)
        except requests.RequestException as exc:
            if attempt == 5:
                raise PremierLeagueApiError(f"Request failed: {url}") from exc
            time.sleep(2**attempt)
            continue

        if response.status_code < 400:
            return response.json()

        if response.status_code == 404:
            return None

        if response.status_code in (429, 500, 502, 503, 504) and attempt < 5:
            time.sleep(2**attempt)
            continue
        seen.add(mid)
        unique_rows.append(row)

        raise PremierLeagueApiError(f"{response.status_code} from {url}: {response.text[:300]}")

    raise PremierLeagueApiError(f"Retry budget exhausted for {url}")

def fetch_match_details(match_id: str | int) -> Dict[str, Any]:
    payload = _get(f"v2/matches/{match_id}")
    return payload if isinstance(payload, dict) else {}

def fetch_matchweek_schedule(season: int, matchweek: int) -> List[Dict[str, Any]]:
    payload = _get(
        f"v1/competitions/{COMPETITION_ID}/seasons/{season}/matchweeks/{matchweek}/matches",
        {"_limit": PAGE_SIZE},
    )
    if not isinstance(payload, dict):
        return []
    data = payload.get("data")
    if isinstance(data, list):
        return data
    return []


def fetch_schedule(season: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    empty_streak = 0
    for matchweek in range(1, MAX_MATCHWEEK + 1):
        batch = fetch_matchweek_schedule(season, matchweek)
        if not batch:
            empty_streak += 1
            if empty_streak >= 5 and matchweek > 5:
                break
            continue

        empty_streak = 0
        for row in batch:
            row["matchWeek"] = row.get("matchWeek", matchweek)
        rows.extend(batch)

    seen: set[str] = set()
    unique_rows: List[Dict[str, Any]] = []
    for row in rows:
        mid = str(row.get("matchId") or "")
        if not mid or mid in seen:
            continue
        seen.add(mid)
        unique_rows.append(row)

    logger.info("schedule season=%s rows=%s", season, len(unique_rows))
    return unique_rows


def fetch_match_details(match_id: str | int) -> Dict[str, Any]:
    payload = _get(f"v2/matches/{match_id}")
    return payload if isinstance(payload, dict) else {}

def fetch_match_details(match_id: str | int) -> Dict[str, Any]:
    payload = _get(f"v2/matches/{match_id}")
    return payload if isinstance(payload, dict) else {}

def fetch_match_stats(match_id: str | int) -> List[Dict[str, Any]]:
    payload = _get(f"v3/matches/{match_id}/stats")
    return payload if isinstance(payload, list) else []


def fetch_match_events(match_id: str | int) -> Dict[str, Any]:
    payload = _get(f"v1/matches/{match_id}/events")
    return payload if isinstance(payload, dict) else {}


def fetch_team_stats(season: int) -> List[Dict[str, Any]]:
    try:
        payload = _get(f"v1/competitions/{COMPETITION_ID}/seasons/{season}/teams", {"_limit": 100})
    except PremierLeagueApiError as exc:
        logger.warning("team_stats unavailable for season=%s: %s", season, exc)
        return []

    if isinstance(payload, dict) and isinstance(payload.get("data"), list):
        return payload["data"]
    return []




def fetch_standings(season: int) -> List[Dict[str, Any]]:
    for path in (
        f"v1/competitions/{COMPETITION_ID}/seasons/{season}/standings",
        f"v1/competitions/{COMPETITION_ID}/seasons/{season}/tables",
    ):
        try:
            payload = _get(path, {"_limit": 100})
        except PremierLeagueApiError as exc:
            logger.warning("standings endpoint failed path=%s season=%s: %s", path, season, exc)
            continue
        if isinstance(payload, dict) and isinstance(payload.get("data"), list):
            return payload["data"]
    return []


def fetch_player_stats(season: int) -> List[Dict[str, Any]]:
    # Endpoint exists but can be unstable / schema variant by stat sort.
    try:
        payload = _get(
            f"v3/competitions/{COMPETITION_ID}/seasons/{season}/players/stats/leaderboard",
            {"_sort": os.getenv("PREMIERLEAGUE_PLAYER_STATS_SORT", "total_passes:desc")},
        )
    except PremierLeagueApiError as exc:
        logger.warning("player_stats unavailable for season=%s: %s", season, exc)
        return []

    if isinstance(payload, dict) and isinstance(payload.get("data"), list):
        return payload["data"]
    return []


def fetch_team_game_stats(season: int, only_date: Optional[date] = None) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for match in fetch_schedule(season):
        kickoff = str(match.get("kickoff") or "")
        if only_date and not kickoff.startswith(only_date.isoformat()):
            continue
        match_id = str(match.get("matchId") or "")
        if not match_id:
            continue
        for side in fetch_match_stats(match_id):
            side["matchId"] = match_id
            rows.append(side)
    return rows


def fetch_player_game_stats(season: int, only_date: Optional[date] = None) -> List[Dict[str, Any]]:
    # Not implemented from Premier League endpoints yet.
    _ = (season, only_date)
    return []

"""
Premier League website API client (MLS-style ingestion contract).

This module mirrors the mlssoccer scraper interface so the broader ingest
pipeline can be reused for additional football leagues.

Default endpoint assumptions target the public API used by
https://www.premierleague.com, but every endpoint is environment-overridable.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from datetime import date
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

BASE_API = os.getenv("PREMIERLEAGUE_API_BASE", "https://footballapi.pulselive.com/football")
DEFAULT_COMPETITION_ID = os.getenv("PREMIERLEAGUE_COMPETITION_ID", "1")

TIMEOUT = 30
PAGE_SIZE = int(os.getenv("PREMIERLEAGUE_PAGE_SIZE", "20"))
RETRY_ATTEMPTS = 6

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


def _competition_id() -> str:
    return os.getenv("PREMIERLEAGUE_COMPETITION_ID", DEFAULT_COMPETITION_ID)


def _season_id_for_year(season: int) -> str:
    """
    Resolve a Premier League `compSeason` id.

    `--season` accepts either:
      - a compSeason id directly (e.g. 578), or
      - a calendar year that is mapped via env vars.

    Mapping options:
      - PREMIERLEAGUE_SEASON_ID_BY_YEAR_JSON='{"2025":"719"}'
      - PREMIERLEAGUE_SEASON_ID_<YEAR>=<id>
      - PREMIERLEAGUE_DEFAULT_COMPSEASON=<id> (fallback)
    """
    if season < 1900:
        return str(season)

    mapping_json = os.getenv("PREMIERLEAGUE_SEASON_ID_BY_YEAR_JSON", "")
    if mapping_json:
        try:
            mapping = json.loads(mapping_json)
            if str(season) in mapping:
                return str(mapping[str(season)])
        except json.JSONDecodeError as exc:
            raise RuntimeError("Invalid PREMIERLEAGUE_SEASON_ID_BY_YEAR_JSON") from exc

    env_value = os.getenv(f"PREMIERLEAGUE_SEASON_ID_{season}")
    if env_value:
        return env_value

    default_compseason = os.getenv("PREMIERLEAGUE_DEFAULT_COMPSEASON")
    if default_compseason:
        return default_compseason

    raise RuntimeError(
        f"No compSeason mapping configured for season={season}. "
        "Set PREMIERLEAGUE_SEASON_ID_<YEAR> (e.g. PREMIERLEAGUE_SEASON_ID_2025=719) "
        "or pass a direct compSeason id like --season 578."
    )


def _get(url: str, params: Optional[Dict[str, Any]] = None) -> Any:
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = _session.get(url, params=params or {}, timeout=TIMEOUT)
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

        if resp.status_code in (429, 500, 502, 503, 504) and attempt < RETRY_ATTEMPTS:
            time.sleep(2 ** attempt)
            continue

        raise RuntimeError(f"HTTP {resp.status_code} from {url}: {resp.text[:200]}")

    raise RuntimeError(f"Failed request to {url}")


def _extract_list(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return payload

    if isinstance(payload, dict):
        for key in (
            "content",
            "results",
            "fixtures",
            "matches",
            "teams",
            "clubs",
            "players",
            "stats",
            "statistics",
            "team_statistics",
            "player_statistics",
            "items",
            "data",
        ):
            if isinstance(payload.get(key), list):
                return payload[key]

        data = payload.get("data")
        if isinstance(data, dict):
            for key in ("items", "results", "matches", "fixtures", "players", "teams"):
                if isinstance(data.get(key), list):
                    return data[key]

    return []


def _paginate(
    url: str,
    base_params: Dict[str, Any],
    page_param: str = "page",
    size_param: str = "pageSize",
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    page = 0
    max_pages = 100

    while page < max_pages:
        params = dict(base_params)
        params[page_param] = page
        params[size_param] = PAGE_SIZE

        payload = _get(url, params)
        batch = _extract_list(payload)
        if not batch:
            break

        rows.extend(batch)

        page_info = payload.get("pageInfo") if isinstance(payload, dict) else None
        if isinstance(page_info, dict):
            num_pages = page_info.get("numPages")
            try:
                if num_pages is not None and page + 1 >= int(num_pages):
                    break
            except (TypeError, ValueError):
                pass

            response_page_size = page_info.get("pageSize")
            try:
                if response_page_size is not None and len(batch) < int(response_page_size):
                    break
            except (TypeError, ValueError):
                pass
        elif len(batch) < PAGE_SIZE:
            break

        page += 1

    return rows


def fetch_schedule(season: int) -> List[Dict[str, Any]]:
    season_id = _season_id_for_year(season)
    url = os.getenv("PREMIERLEAGUE_SCHEDULE_URL", f"{BASE_API}/fixtures")

    params = {
        "comps": _competition_id(),
        "compSeasons": season_id,
        "sort": "desc",
    }
    rows = _paginate(url, params)
    logger.info("schedule: %d fixtures", len(rows))
    return rows


def fetch_team_stats(season: int) -> List[Dict[str, Any]]:
    season_id = _season_id_for_year(season)
    url = os.getenv("PREMIERLEAGUE_TEAM_STATS_URL", f"{BASE_API}/stats/team")
    params = {"comps": _competition_id(), "compSeasons": season_id}
    rows = _extract_list(_get(url, params))
    logger.info("team_stats: %d rows", len(rows))
    return rows


def fetch_player_stats(season: int) -> List[Dict[str, Any]]:
    season_id = _season_id_for_year(season)
    url = os.getenv("PREMIERLEAGUE_PLAYER_STATS_URL", f"{BASE_API}/stats/player")
    params = {"comps": _competition_id(), "compSeasons": season_id}
    rows = _extract_list(_get(url, params))
    logger.info("player_stats: %d rows", len(rows))
    return rows


def _is_match_completed(match: Dict[str, Any]) -> bool:
    status = str(
        match.get("status")
        or match.get("statusShort")
        or match.get("match_status")
        or ""
    ).lower()

    if status in {
        "ft",
        "aet",
        "final",
        "full time",
        "completed",
        "finished",
        "post-match",
    }:
        return True

    home_score = match.get("team1Score") or match.get("home_team_goals")
    away_score = match.get("team2Score") or match.get("away_team_goals")
    return home_score is not None and away_score is not None


def _fetch_stats_for_match(base_url: str, match_id: str) -> List[Dict[str, Any]]:
    for filter_key in ("matchId", "match_id", "fixture", "fixtureId"):
        rows = _extract_list(_get(base_url, {filter_key: match_id, "pageSize": PAGE_SIZE}))
        if rows:
            return rows
    return []


def fetch_team_game_stats(season: int, only_date: Optional[date] = None) -> List[Dict[str, Any]]:
    matches = fetch_schedule(season)
    completed = [m for m in matches if _is_match_completed(m)]

    if only_date is not None:
        only_str = only_date.isoformat()
        completed = [
            m
            for m in completed
            if str(m.get("kickoff") or m.get("date") or m.get("match_date") or "").startswith(only_str)
        ]

    base_url = os.getenv("PREMIERLEAGUE_TEAM_GAME_STATS_URL", f"{BASE_API}/stats/team")

    all_rows: List[Dict[str, Any]] = []
    for match in completed:
        match_id = str(match.get("id") or match.get("matchId") or match.get("fixtureId") or "")
        if not match_id:
            continue

        rows = _fetch_stats_for_match(base_url, match_id)
        match_date = str(match.get("kickoff") or match.get("date") or match.get("match_date") or "")

        for row in rows:
            row.setdefault("match_id", match_id)
            row.setdefault("match_date", match_date)

        all_rows.extend(rows)
        time.sleep(0.05)

    logger.info("team_game_stats: %d rows", len(all_rows))
    return all_rows


def fetch_player_game_stats(season: int, only_date: Optional[date] = None) -> List[Dict[str, Any]]:
    matches = fetch_schedule(season)
    completed = [m for m in matches if _is_match_completed(m)]

    if only_date is not None:
        only_str = only_date.isoformat()
        completed = [
            m
            for m in completed
            if str(m.get("kickoff") or m.get("date") or m.get("match_date") or "").startswith(only_str)
        ]

    base_url = os.getenv("PREMIERLEAGUE_PLAYER_GAME_STATS_URL", f"{BASE_API}/stats/player")

    all_rows: List[Dict[str, Any]] = []
    for match in completed:
        match_id = str(match.get("id") or match.get("matchId") or match.get("fixtureId") or "")
        if not match_id:
            continue

        rows = _fetch_stats_for_match(base_url, match_id)
        match_date = str(match.get("kickoff") or match.get("date") or match.get("match_date") or "")

        for row in rows:
            row.setdefault("match_id", match_id)
            row.setdefault("match_date", match_date)

        all_rows.extend(rows)
        time.sleep(0.05)

    logger.info("player_game_stats: %d rows", len(all_rows))
    return all_rows


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch Premier League website data")
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument(
        "--data",
        choices=["schedule", "team_stats", "player_stats", "team_game_stats", "player_game_stats", "all"],
        default="all",
    )
    parser.add_argument("--json", action="store_true")
    return parser


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

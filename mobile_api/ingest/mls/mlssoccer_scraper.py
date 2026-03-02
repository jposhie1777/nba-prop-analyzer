"""
Modern MLS public API client (production-safe).

Confirmed working endpoints:

Season schedule:
  stats-api  /matches/seasons/{season_id}
    ?match_date[gte]=YYYY-MM-DD  &match_date[lte]=YYYY-MM-DD

Team season stats:
  stats-api  /statistics/clubs/competitions/{competition_id}/seasons/{season_id}

Player season stats:
  stats-api  /statistics/players/competitions/{competition_id}/seasons/{season_id}

Per-match club/player stats:
  sportapi   /api/matches/bySportecIds/{id1},{id2},...
               Bulk-fetch up to SPORT_API_BATCH_SIZE matches per request.
               Returns a list of full match objects (scores, team/player data).
  sportapi   /api/matches/{match_id}
               Per-match fallback when a match is absent from the bulk response.
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
SPORT_API_BATCH_SIZE = 50  # max IDs per bySportecIds request

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


def _is_match_completed(match: Dict[str, Any]) -> bool:
    """Return True when the match result is finalized."""
    status = (
        match.get("match_status")
        or match.get("status")
        or ""
    ).lower()

    if status in {"finalwhistle", "final", "ft", "completed", "finished"}:
        return True

    # Fallback: MLS returns score fields only for completed matches
    if match.get("home_team_goals") is not None and match.get("away_team_goals") is not None:
        return True

    return False


def _match_id_from_row(row: Dict[str, Any]) -> str:
    return str(
        row.get("id")
        or row.get("match_id")
        or row.get("matchId")
        or row.get("optaId")
        or row.get("opta_id")
        or ""
    )


# ---------------------------------------------------------------------------
# sportapi bulk / single match fetch
# ---------------------------------------------------------------------------


def _fetch_matches_bulk_sportapi(match_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Fetch full match objects from sportapi.mlssoccer.com.

    Primary:  /api/matches/bySportecIds/{id1},{id2},...  (batched)
    Fallback: /api/matches/{match_id}  for any IDs not returned by the batch.

    Returns a dict keyed by the match_id string found in each response object.
    """
    result: Dict[str, Dict[str, Any]] = {}

    for i in range(0, len(match_ids), SPORT_API_BATCH_SIZE):
        batch = match_ids[i : i + SPORT_API_BATCH_SIZE]
        ids_str = ",".join(batch)
        url = f"{SPORT_API}/api/matches/bySportecIds/{ids_str}"

        try:
            payload = _get(url)
        except RuntimeError as exc:
            logger.warning("bySportecIds batch [%d:%d] failed: %s", i, i + len(batch), exc)
            payload = {}

        for m in _extract_list(payload):
            mid = _match_id_from_row(m)
            if mid:
                result[mid] = m

        # Per-match fallback for any IDs missing from the bulk response
        for mid in batch:
            if mid in result:
                continue
            try:
                m = _get(f"{SPORT_API}/api/matches/{mid}")
                if isinstance(m, dict) and m:
                    key = _match_id_from_row(m) or mid
                    result[key] = m
            except RuntimeError:
                pass
            time.sleep(0.05)

        time.sleep(0.1)  # brief pause between batch requests

    return result


# ---------------------------------------------------------------------------
# Extraction helpers for sportapi match objects
# ---------------------------------------------------------------------------


def _team_rows_from_sportapi_match(
    sportapi_match: Dict[str, Any],
    match_id: str,
    match_date: str,
) -> List[Dict[str, Any]]:
    """
    Extract one row per team (home + away) from a sportapi match object.

    The match object from sportapi.mlssoccer.com/api/matches/{id} embeds both
    teams' data (scores, statistics, etc.) under homeTeam / awayTeam keys.
    Each extracted row is tagged with match_id, match_date, and side.
    """
    rows: List[Dict[str, Any]] = []
    for side in ("home", "away"):
        team_obj = (
            sportapi_match.get(f"{side}Team")
            or sportapi_match.get(f"{side}_team")
        )
        if not isinstance(team_obj, dict):
            continue
        row = dict(team_obj)
        row.setdefault("match_id", match_id)
        row.setdefault("match_date", match_date)
        row["side"] = side
        rows.append(row)
    return rows


def _player_rows_from_sportapi_match(
    sportapi_match: Dict[str, Any],
    match_id: str,
    match_date: str,
) -> List[Dict[str, Any]]:
    """
    Extract one row per player from a sportapi match object.

    Handles several nesting patterns the sportapi uses:
      - lineups.homeTeam / lineups.awayTeam  (list of player objects)
      - homeTeam.players / awayTeam.players
      - top-level players list
    """
    rows: List[Dict[str, Any]] = []

    def _add_players(player_list: Any, side: str) -> None:
        if not isinstance(player_list, list):
            return
        for p in player_list:
            if not isinstance(p, dict):
                continue
            row = dict(p)
            row.setdefault("match_id", match_id)
            row.setdefault("match_date", match_date)
            row.setdefault("side", side)
            rows.append(row)

    # Pattern 1: lineups dict at top level
    lineups = sportapi_match.get("lineups") or sportapi_match.get("lineup")
    if isinstance(lineups, dict):
        _add_players(
            lineups.get("homeTeam") or lineups.get("home"),
            "home",
        )
        _add_players(
            lineups.get("awayTeam") or lineups.get("away"),
            "away",
        )

    # Pattern 2: players nested inside homeTeam / awayTeam
    if not rows:
        for side in ("home", "away"):
            team_obj = (
                sportapi_match.get(f"{side}Team")
                or sportapi_match.get(f"{side}_team")
                or {}
            )
            _add_players(
                team_obj.get("players") or team_obj.get("lineup"),
                side,
            )

    # Pattern 3: top-level players list
    if not rows:
        _add_players(
            sportapi_match.get("players") or sportapi_match.get("playerStatistics"),
            "",
        )

    return rows


# ---------------------------------------------------------------------------
# Team game stats (per-club per-match)
# ---------------------------------------------------------------------------


def fetch_team_game_stats(season: int, only_date: Optional[date] = None) -> List[Dict[str, Any]]:
    """
    Fetch per-club per-match statistics for *season*.

    Uses sportapi.mlssoccer.com/api/matches/bySportecIds (bulk) to retrieve
    full match objects for all completed matches, then extracts the homeTeam
    and awayTeam sub-objects as individual rows.  Falls back to per-match
    requests for any IDs absent from the bulk response.

    Parameters
    ----------
    only_date: If provided, only process matches whose match_date equals this date.
    """
    matches = fetch_schedule(season)
    completed = [m for m in matches if _is_match_completed(m)]
    if only_date is not None:
        only_str = only_date.isoformat()
        completed = [
            m for m in completed
            if (
                m.get("match_date")
                or m.get("date")
                or m.get("matchDate")
                or m.get("planned_kickoff_time")
                or ""
            ).startswith(only_str)
        ]
    logger.info("team_game_stats: %d completed matches to process", len(completed))

    # Build ordered match_id list and schedule lookup (deduplicated)
    seen: set = set()
    ordered_ids: List[str] = []
    schedule_by_id: Dict[str, Dict[str, Any]] = {}
    for m in completed:
        mid = str(m.get("match_id") or m.get("id") or m.get("matchId") or "")
        if mid and mid not in seen:
            seen.add(mid)
            ordered_ids.append(mid)
            schedule_by_id[mid] = m

    sportapi_data = _fetch_matches_bulk_sportapi(ordered_ids)
    logger.info(
        "team_game_stats: sportapi returned %d/%d matches",
        len(sportapi_data), len(ordered_ids),
    )

    all_rows: List[Dict[str, Any]] = []
    no_stats_count = 0
    for mid in ordered_ids:
        schedule_row = schedule_by_id[mid]
        match_date = (
            schedule_row.get("match_date")
            or schedule_row.get("date")
            or schedule_row.get("matchDate")
            or schedule_row.get("planned_kickoff_time")
            or ""
        )
        sportapi_match = sportapi_data.get(mid)
        if not sportapi_match:
            logger.info("No per-match stats found for match_id=%s", mid)
            no_stats_count += 1
            continue

        rows = _team_rows_from_sportapi_match(sportapi_match, mid, match_date)
        if not rows:
            logger.info("No team rows extracted for match_id=%s", mid)
            no_stats_count += 1
        all_rows.extend(rows)

    if no_stats_count:
        logger.info(
            "team_game_stats: %d/%d completed matches had no per-match stats",
            no_stats_count, len(ordered_ids),
        )
    logger.info("team_game_stats: %d rows total", len(all_rows))
    return all_rows


# ---------------------------------------------------------------------------
# Player game stats (per-player per-match)
# ---------------------------------------------------------------------------


def fetch_player_game_stats(season: int, only_date: Optional[date] = None) -> List[Dict[str, Any]]:
    """
    Fetch per-player per-match statistics for *season*.

    Uses the same sportapi bulk strategy as fetch_team_game_stats but extracts
    per-player rows from each match object's lineups / player data.

    Parameters
    ----------
    only_date: If provided, only process matches whose match_date equals this date.
    """
    matches = fetch_schedule(season)
    completed = [m for m in matches if _is_match_completed(m)]
    if only_date is not None:
        only_str = only_date.isoformat()
        completed = [
            m for m in completed
            if (
                m.get("match_date")
                or m.get("date")
                or m.get("matchDate")
                or m.get("planned_kickoff_time")
                or ""
            ).startswith(only_str)
        ]
    logger.info("player_game_stats: %d completed matches to process", len(completed))

    seen: set = set()
    ordered_ids: List[str] = []
    schedule_by_id: Dict[str, Dict[str, Any]] = {}
    for m in completed:
        mid = str(m.get("match_id") or m.get("id") or m.get("matchId") or "")
        if mid and mid not in seen:
            seen.add(mid)
            ordered_ids.append(mid)
            schedule_by_id[mid] = m

    sportapi_data = _fetch_matches_bulk_sportapi(ordered_ids)
    logger.info(
        "player_game_stats: sportapi returned %d/%d matches",
        len(sportapi_data), len(ordered_ids),
    )

    all_rows: List[Dict[str, Any]] = []
    no_stats_count = 0
    for mid in ordered_ids:
        schedule_row = schedule_by_id[mid]
        match_date = (
            schedule_row.get("match_date")
            or schedule_row.get("date")
            or schedule_row.get("matchDate")
            or schedule_row.get("planned_kickoff_time")
            or ""
        )
        sportapi_match = sportapi_data.get(mid)
        if not sportapi_match:
            logger.info("No per-match player stats found for match_id=%s", mid)
            no_stats_count += 1
            continue

        rows = _player_rows_from_sportapi_match(sportapi_match, mid, match_date)
        if not rows:
            logger.info("No player rows extracted for match_id=%s", mid)
            no_stats_count += 1
        all_rows.extend(rows)

    if no_stats_count:
        logger.info(
            "player_game_stats: %d/%d completed matches had no per-match stats",
            no_stats_count, len(ordered_ids),
        )
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

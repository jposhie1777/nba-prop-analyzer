"""
PGA Tour player stats scraper using player profile stats endpoint.

Primary endpoint shape (per player):
  GET https://data-api.pgatour.com/player/profiles/{player_id}/stats

The parser is intentionally defensive because PGA website payload shapes can evolve.
It attempts known stat-list paths first, then falls back to recursive extraction.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

PLAYER_STATS_ENDPOINT = "https://data-api.pgatour.com/player/profiles/{player_id}/stats"
DEFAULT_TIMEOUT = 20


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class StatPlayerRow:
    """One player's value for one stat in a given year."""

    stat_id: str
    stat_name: str
    player_id: str
    player_name: str
    stat_title: str
    stat_value: str
    rank: int
    country: Optional[str]
    country_flag: Optional[str]
    tour_avg: Optional[str]
    tour_code: str
    year: int


@dataclass
class StatCategory:
    category: str
    display_name: str
    sub_categories: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class StatOverviewResult:
    tour_code: str
    year: int
    categories: List[StatCategory] = field(default_factory=list)
    players: List[StatPlayerRow] = field(default_factory=list)


# ---------------------------------------------------------------------------
# HTTP helper
# ---------------------------------------------------------------------------


def _headers() -> Dict[str, str]:
    return {
        "Accept": "application/json",
        "Referer": "https://www.pgatour.com/",
        "Origin": "https://www.pgatour.com",
        "User-Agent": "Mozilla/5.0",
    }


def fetch_player_stats_raw(
    player_id: str,
    *,
    timeout: int = DEFAULT_TIMEOUT,
) -> Dict[str, Any]:
    url = PLAYER_STATS_ENDPOINT.format(player_id=player_id)
    resp = requests.get(url, headers=_headers(), timeout=timeout)
    resp.raise_for_status()
    payload = resp.json()
    return payload if isinstance(payload, dict) else {"data": payload}


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


def _safe_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    if isinstance(v, bool):
        return None
    try:
        return int(str(v).strip())
    except (TypeError, ValueError):
        return None


def _pick(d: Dict[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        if key in d and d[key] not in (None, ""):
            return d[key]
    return None


def _normalize_player_name(payload: Dict[str, Any], fallback: str = "") -> str:
    for node in [payload, payload.get("player"), payload.get("profile"), payload.get("data")]:
        if not isinstance(node, dict):
            continue
        name = _pick(node, ["displayName", "playerName", "name", "fullName"])
        if name:
            return str(name)
        first = _pick(node, ["firstName", "first_name"]) or ""
        last = _pick(node, ["lastName", "last_name"]) or ""
        full = " ".join(x for x in [str(first).strip(), str(last).strip()] if x)
        if full:
            return full
    return fallback


def _normalize_country(payload: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    for node in [payload, payload.get("player"), payload.get("profile"), payload.get("data")]:
        if not isinstance(node, dict):
            continue
        country = _pick(node, ["country", "countryName", "nationality"])
        flag = _pick(node, ["countryFlag", "countryCode", "country_code"])
        if country or flag:
            return (str(country) if country is not None else None, str(flag) if flag is not None else None)
    return (None, None)


def _looks_like_stat_row(item: Dict[str, Any]) -> bool:
    has_stat_identity = any(k in item for k in ["statId", "stat_id", "id", "statCode", "key", "statKey"])
    has_value = any(k in item for k in ["statValue", "value", "displayValue", "playerValue", "currentValue"])
    has_title = any(k in item for k in ["statName", "name", "title", "statTitle", "label"])
    return has_value and (has_stat_identity or has_title)


def _extract_rows_recursive(node: Any, out: List[Dict[str, Any]]) -> None:
    if isinstance(node, dict):
        if _looks_like_stat_row(node):
            out.append(node)
        for value in node.values():
            _extract_rows_recursive(value, out)
    elif isinstance(node, list):
        for item in node:
            _extract_rows_recursive(item, out)


def _extract_candidate_rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []

    known_paths = [
        ("stats",),
        ("data", "stats"),
        ("playerStats",),
        ("data", "playerStats"),
        ("statCategories",),
        ("data", "statCategories"),
        ("seasons",),
        ("data", "seasons"),
    ]

    for path in known_paths:
        node: Any = payload
        ok = True
        for key in path:
            if not isinstance(node, dict) or key not in node:
                ok = False
                break
            node = node[key]
        if ok:
            _extract_rows_recursive(node, candidates)

    if not candidates:
        _extract_rows_recursive(payload, candidates)

    # de-dupe raw dict identity-ish signature
    deduped: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for row in candidates:
        sig = json.dumps(row, sort_keys=True, default=str)
        if sig in seen:
            continue
        seen.add(sig)
        deduped.append(row)
    return deduped


def _year_from_row(row: Dict[str, Any]) -> Optional[int]:
    raw = _pick(row, ["year", "season", "seasonYear", "statYear"])
    if raw is None:
        return None
    match = re.search(r"(20\d{2})", str(raw))
    if not match:
        return None
    return _safe_int(match.group(1))


def _to_stat_player_row(
    row: Dict[str, Any],
    *,
    player_id: str,
    player_name: str,
    country: Optional[str],
    country_flag: Optional[str],
    tour_code: str,
    year: int,
) -> Optional[StatPlayerRow]:
    row_year = _year_from_row(row)
    if row_year is not None and row_year != year:
        return None

    stat_id = _pick(row, ["statId", "stat_id", "statCode", "statKey", "key", "id"]) or ""
    stat_name = _pick(row, ["statName", "name", "title", "label", "statTitle"]) or ""
    stat_title = _pick(row, ["statTitle", "title", "label", "statLabel"]) or stat_name
    stat_value = _pick(row, ["statValue", "displayValue", "value", "playerValue", "currentValue"]) or ""
    rank = _safe_int(_pick(row, ["rank", "position", "statRank"])) or 0
    tour_avg = _pick(row, ["tourAvg", "tourAverage", "pgaTourAverage", "fieldAverage", "avg", "average"])

    if not stat_value and not stat_name and not stat_id:
        return None

    return StatPlayerRow(
        stat_id=str(stat_id),
        stat_name=str(stat_name),
        player_id=str(player_id),
        player_name=str(player_name),
        stat_title=str(stat_title),
        stat_value=str(stat_value),
        rank=rank,
        country=country,
        country_flag=country_flag,
        tour_avg=(str(tour_avg) if tour_avg is not None else None),
        tour_code=tour_code,
        year=year,
    )


def parse_player_stats(
    player_id: str,
    payload: Dict[str, Any],
    *,
    tour_code: str,
    year: int,
    fallback_name: str = "",
) -> List[StatPlayerRow]:
    player_name = _normalize_player_name(payload, fallback=fallback_name or str(player_id))
    country, country_flag = _normalize_country(payload)

    raw_rows = _extract_candidate_rows(payload)
    rows: List[StatPlayerRow] = []
    for raw in raw_rows:
        parsed = _to_stat_player_row(
            raw,
            player_id=player_id,
            player_name=player_name,
            country=country,
            country_flag=country_flag,
            tour_code=tour_code,
            year=year,
        )
        if parsed is not None:
            rows.append(parsed)

    # Final row-level dedupe for noisy payloads.
    deduped: List[StatPlayerRow] = []
    seen_keys: set[Tuple[str, str, str, int]] = set()
    for row in rows:
        key = (row.player_id, row.stat_id or row.stat_name, row.stat_value, row.rank)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        deduped.append(row)
    return deduped


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def fetch_stat_overview_for_players(
    player_ids: List[str],
    *,
    tour_code: str = "R",
    year: int,
    retries: int = 3,
    sleep_s: float = 0.05,
) -> StatOverviewResult:
    players: List[StatPlayerRow] = []

    for player_id in player_ids:
        backoff = 1.0
        last_exc: Optional[Exception] = None
        for attempt in range(retries):
            try:
                payload = fetch_player_stats_raw(str(player_id))
                players.extend(
                    parse_player_stats(str(player_id), payload, tour_code=tour_code, year=year)
                )
                time.sleep(sleep_s)
                break
            except requests.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else 0
                if status in (429, 500, 502, 503, 504) and attempt < retries - 1:
                    last_exc = exc
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                raise
            except Exception as exc:
                last_exc = exc
                if attempt < retries - 1:
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                raise RuntimeError(
                    f"Failed fetching stats for player_id={player_id}. Last error: {last_exc}"
                ) from exc

    return StatOverviewResult(tour_code=tour_code, year=year, categories=[], players=players)


def fetch_stat_overview(
    tour_code: str = "R",
    year: int = 2025,
    *,
    player_ids: Optional[List[str]] = None,
) -> StatOverviewResult:
    """Fetch player stats for the provided player list (or empty list)."""
    return fetch_stat_overview_for_players(player_ids or [], tour_code=tour_code, year=year)


def fetch_stat_overview_raw(
    tour_code: str = "R",
    year: int = 2025,
    *,
    player_ids: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Return raw payloads keyed by player_id for debugging."""
    out: Dict[str, Any] = {}
    for player_id in player_ids or []:
        out[str(player_id)] = fetch_player_stats_raw(str(player_id))
    return {"tour_code": tour_code, "year": year, "players": out}


def stat_players_to_records(
    result: StatOverviewResult,
    *,
    run_ts: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Flatten StatOverviewResult into BigQuery-ready rows."""
    from datetime import datetime

    ts = run_ts or datetime.utcnow().isoformat()
    return [
        {
            "run_ts": ts,
            "ingested_at": ts,
            "tour_code": p.tour_code,
            "year": p.year,
            "stat_id": p.stat_id,
            "stat_name": p.stat_name,
            "tour_avg": p.tour_avg,
            "player_id": p.player_id,
            "player_name": p.player_name,
            "stat_title": p.stat_title,
            "stat_value": p.stat_value,
            "rank": p.rank,
            "country": p.country,
            "country_flag": p.country_flag,
        }
        for p in result.players
    ]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _cli() -> None:
    parser = argparse.ArgumentParser(description="Fetch PGA player stats from profile endpoint.")
    parser.add_argument("--tour", default="R", metavar="TOUR_CODE")
    parser.add_argument("--year", type=int, default=2025, metavar="YEAR")
    parser.add_argument("--player-id", action="append", dest="player_ids")
    parser.add_argument("--raw", action="store_true")
    parser.add_argument("--json", action="store_true", dest="as_json")
    args = parser.parse_args()

    if not args.player_ids:
        print("Provide at least one --player-id for CLI usage.", file=sys.stderr)
        sys.exit(2)

    if args.raw:
        data = fetch_stat_overview_raw(args.tour, args.year, player_ids=args.player_ids)
        print(json.dumps(data, indent=2, default=str))
        return

    result = fetch_stat_overview(args.tour, args.year, player_ids=args.player_ids)
    if args.as_json:
        print(json.dumps(stat_players_to_records(result), indent=2, default=str))
        return

    print(f"Fetched {len(result.players)} stat rows for {len(args.player_ids)} players.")


if __name__ == "__main__":
    _cli()

"""
PGA Tour per-player profile stats scraper.

Fetches each player's stats page (https://www.pgatour.com/player/{id}/{name}/stats)
and extracts the ``statsOverview`` array embedded in the Next.js __NEXT_DATA__ blob.

The ``statsOverview`` field contains ~14 curated key stats per player with their
value, rank, rank deviation, and category tags.  This approach is player-centric
and captures stats for every active player regardless of whether they appear in
stat leaderboards (which require a minimum number of rounds).

Usage (standalone CLI):
    python pga_player_profile_stats_scraper.py --player-id 52955 --player-name "Ludvig Aberg"
    python pga_player_profile_stats_scraper.py --player-id 46046 --player-name "Scottie Scheffler" --season 2026

Usage (as a module):
    from ingest.pga.pga_player_profile_stats_scraper import (
        fetch_player_profile_stats,
        profile_stats_to_records,
    )
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
import urllib.parse
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests

STATS_PAGE_BASE = "https://www.pgatour.com/player"
DEFAULT_TIMEOUT = 30


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class PlayerProfileStatRow:
    """One stat entry from a player's profile overview."""

    player_id: str
    player_name: str
    tour_code: str
    season: int
    stat_id: str
    stat_title: str
    stat_value: str
    rank: Optional[int]
    rank_deviation: Optional[float]
    above_or_below: Optional[str]
    categories: str  # JSON-encoded list, e.g. '["STROKES_GAINED", "SCORING"]'


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------


def _browser_headers() -> Dict[str, str]:
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/145.0.0.0 Safari/537.36"
        ),
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;"
            "q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,"
            "application/signed-exchange;v=b3;q=0.7"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://www.pgatour.com/",
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "same-origin",
    }


def _name_to_url_slug(name: str) -> str:
    """
    Convert a player display name to the URL slug used by pgatour.com.

    Examples:
        "Scottie Scheffler" -> "Scottie-Scheffler"
        "Ludvig Åberg"      -> "Ludvig-%C3%85berg"
    """
    slug = name.replace(" ", "-")
    return urllib.parse.quote(slug, safe="-")


# ---------------------------------------------------------------------------
# HTML parsing
# ---------------------------------------------------------------------------


def _extract_next_data(html: str) -> Dict[str, Any]:
    """Extract and parse the Next.js __NEXT_DATA__ JSON block from a page."""
    match = re.search(
        r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
        html,
        re.DOTALL,
    )
    if not match:
        raise ValueError("No __NEXT_DATA__ block found in page HTML")
    return json.loads(match.group(1))


def _find_stats_overview(
    next_data: Dict[str, Any],
    season: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Navigate the dehydrated React Query state to find ``statsOverview``.

    The page embeds a ``dehydratedState.queries`` array.  Each query has a
    ``queryKey`` like ``["playerProfileStats", {playerId, season, tourCode}]``.
    We look for the ``playerProfileStats`` query whose season matches the
    requested season, falling back to any query that has ``statsOverview``.
    """
    try:
        queries: List[Dict] = (
            next_data["props"]["pageProps"]["dehydratedState"]["queries"]
        )
    except (KeyError, TypeError):
        return []

    # First pass: prefer query with matching season
    if season is not None:
        for query in queries:
            key = query.get("queryKey") or []
            if not key or key[0] != "playerProfileStats":
                continue
            params: Dict = key[1] if len(key) > 1 else {}
            if str(params.get("season") or "") != str(season):
                continue
            data = (query.get("state") or {}).get("data") or {}
            if "statsOverview" in data:
                return data["statsOverview"]

    # Second pass: any playerProfileStats with statsOverview
    for query in queries:
        key = query.get("queryKey") or []
        if key and key[0] == "playerProfileStats":
            data = (query.get("state") or {}).get("data") or {}
            if "statsOverview" in data:
                return data["statsOverview"]

    return []


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


def _safe_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(str(v).replace("T", "").strip())
    except (ValueError, TypeError):
        return None


def _safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def fetch_player_profile_stats(
    player_id: str,
    player_name: str,
    season: int = 2026,
    tour_code: str = "R",
    *,
    retries: int = 3,
    timeout: int = DEFAULT_TIMEOUT,
) -> List[PlayerProfileStatRow]:
    """
    Fetch the profile stats overview for a single player.

    Args:
        player_id:   PGA Tour player ID (numeric string), e.g. ``"52955"``.
        player_name: Player display name, e.g. ``"Ludvig Åberg"``.
        season:      Season year, e.g. ``2026``.
        tour_code:   Tour code, e.g. ``"R"`` (PGA Tour).
        retries:     Number of retry attempts on transient HTTP failures.
        timeout:     Request timeout in seconds.

    Returns:
        List of :class:`PlayerProfileStatRow` (one per stat in the overview).
        Returns an empty list if the player page has no stats data.
    """
    url_slug = _name_to_url_slug(player_name)
    url = f"{STATS_PAGE_BASE}/{player_id}/{url_slug}/stats"

    backoff = 2
    last_exc: Optional[Exception] = None
    resp: Optional[requests.Response] = None

    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=_browser_headers(), timeout=timeout)
            if resp.status_code == 404:
                return []
            resp.raise_for_status()
            break
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else 0
            if status in (429, 500, 502, 503, 504):
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
            raise

    if resp is None:
        raise RuntimeError(f"Exceeded {retries} retries. Last error: {last_exc}")

    try:
        next_data = _extract_next_data(resp.text)
    except (ValueError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            f"Failed to parse __NEXT_DATA__ for player {player_id}: {exc}"
        ) from exc

    raw_stats = _find_stats_overview(next_data, season=str(season))

    rows: List[PlayerProfileStatRow] = []
    for stat in raw_stats:
        rows.append(
            PlayerProfileStatRow(
                player_id=player_id,
                player_name=player_name,
                tour_code=tour_code,
                season=season,
                stat_id=str(stat.get("statId") or ""),
                stat_title=str(stat.get("title") or ""),
                stat_value=str(stat.get("value") or ""),
                rank=_safe_int(stat.get("rank")),
                rank_deviation=_safe_float(stat.get("rankDeviation")),
                above_or_below=stat.get("aboveOrBelow"),
                categories=json.dumps(stat.get("category") or []),
            )
        )
    return rows


def profile_stats_to_records(
    rows: List[PlayerProfileStatRow],
    *,
    run_ts: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Flatten PlayerProfileStatRow list into BigQuery-ready row dicts.

    Produces one row per (player, stat).
    """
    from datetime import datetime

    ts = run_ts or datetime.utcnow().isoformat()
    return [
        {
            "run_ts": ts,
            "ingested_at": ts,
            "tour_code": r.tour_code,
            "season": r.season,
            "player_id": r.player_id,
            "player_name": r.player_name,
            "stat_id": r.stat_id,
            "stat_title": r.stat_title,
            "stat_value": r.stat_value,
            "rank": r.rank,
            "rank_deviation": r.rank_deviation,
            "above_or_below": r.above_or_below,
            "categories": r.categories,
        }
        for r in rows
    ]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _cli() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch PGA Tour player profile stats from pgatour.com."
    )
    parser.add_argument("--player-id", required=True, metavar="ID",
                        help="PGA Tour player ID, e.g. 52955")
    parser.add_argument("--player-name", required=True, metavar="NAME",
                        help='Player display name, e.g. "Ludvig Aberg"')
    parser.add_argument("--season", type=int, default=2026, metavar="YEAR",
                        help="Season year (default: 2026)")
    parser.add_argument("--tour", default="R", metavar="TOUR_CODE",
                        help="Tour code (default: R)")
    parser.add_argument("--json", action="store_true", dest="as_json",
                        help="Output flat JSON records")
    args = parser.parse_args()

    print(
        f"Fetching profile stats: player_id={args.player_id} "
        f"name='{args.player_name}' season={args.season}",
        file=sys.stderr,
    )

    rows = fetch_player_profile_stats(
        player_id=args.player_id,
        player_name=args.player_name,
        season=args.season,
        tour_code=args.tour,
    )

    if not rows:
        print("No stats found for this player / season.", file=sys.stderr)
        sys.exit(1)

    if args.as_json:
        print(json.dumps(profile_stats_to_records(rows), indent=2, default=str))
        return

    print(f"\n  {rows[0].player_name} — {rows[0].season} ({rows[0].tour_code})\n")
    print(f"  {'Stat':30}  {'Value':>10}  {'Rank':>6}  {'±':>6}  Categories")
    print("  " + "-" * 75)
    for r in rows:
        cats = ", ".join(json.loads(r.categories))
        dev = f"{r.rank_deviation:+.3f}" if r.rank_deviation is not None else ""
        print(
            f"  {r.stat_title:30}  {r.stat_value:>10}  "
            f"{'#' + str(r.rank) if r.rank else '':>6}  {dev:>6}  {cats}"
        )


if __name__ == "__main__":
    _cli()

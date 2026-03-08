"""
PGA Tour per-player tournament scorecard history scraper.

Fetches each player's results page
(https://www.pgatour.com/player/{id}/{slug}/results)
and extracts the ``resultsData`` embedded in the Next.js __NEXT_DATA__ blob.

``resultsData`` contains one row per completed tournament with actual stroke
counts for R1, R2, R3, R4, the total strokes, position, and score-to-par.

The ``fields`` array layout per tournament entry:
    [0]  Date            e.g. "1.15.2026"
    [1]  Tournament name e.g. "Sony Open in Hawaii"
    [2]  Position        e.g. "T6", "CUT", "WD"
    [3]  R1 strokes      e.g. "66"  (or "-" when round not played)
    [4]  R2 strokes
    [5]  R3 strokes
    [6]  R4 strokes
    [7]  Total strokes   e.g. "269" (or partial total after cut)
    [8]  To par          e.g. "-11", "+2", "E"
    [9+] FedExCup rank, pts, FedexCup Fall rank, pts, Winnings

Usage (standalone CLI):
    python player_scorecard_scraper.py --player-id 40026 --player-name "Daniel Berger"
    python player_scorecard_scraper.py --player-id 46046 --player-name "Scottie Scheffler" --season 2025

Usage (as a module):
    from ingest.pga.player_scorecard_scraper import (
        fetch_player_scorecard_history,
        scorecard_history_to_records,
    )
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
import urllib.parse
import unicodedata
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests

RESULTS_PAGE_BASE = "https://www.pgatour.com/player"
DEFAULT_TIMEOUT = 30

# Tracks seasons for which we've already emitted a structure-change debug message,
# so we don't spam 214 identical lines per season.
_DEBUG_LOGGED_SEASONS: set = set()


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class PlayerTournamentScorecard:
    """One tournament scorecard entry from a player's results history."""

    player_id: str
    player_name: str
    season: int
    tournament_id: str
    tournament_name: str
    tournament_date: str
    position: str
    r1: Optional[int]
    r2: Optional[int]
    r3: Optional[int]
    r4: Optional[int]
    total_strokes: Optional[int]
    to_par: Optional[str]


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


def _name_to_ascii_slug(name: str) -> str:
    """Return a lower-case ASCII slug for newer PGA Tour URL variants."""
    normalized = unicodedata.normalize("NFKD", name)
    ascii_name = normalized.encode("ascii", "ignore").decode("ascii")
    cleaned = re.sub(r"[^A-Za-z0-9\- ]+", "", ascii_name)
    collapsed = re.sub(r"\s+", "-", cleaned.strip())
    return collapsed.lower()


def _candidate_results_urls(player_id: str, player_name: str) -> List[str]:
    """
    Build resilient URL candidates for PGA results pages.

    PGA occasionally changes URL casing/path conventions. We try a few known
    variants so a single convention change does not zero out all rows.
    """
    display_slug = _name_to_url_slug(player_name)
    ascii_slug = _name_to_ascii_slug(player_name)

    candidates = [
        f"https://www.pgatour.com/player/{player_id}/{display_slug}/results",
        f"https://www.pgatour.com/player/{player_id}/{ascii_slug}/results",
        f"https://www.pgatour.com/players/{player_id}/{ascii_slug}/results",
        f"https://www.pgatour.com/player/{player_id}/results",
    ]

    # Deduplicate while preserving order.
    deduped: List[str] = []
    seen = set()
    for url in candidates:
        if url in seen:
            continue
        seen.add(url)
        deduped.append(url)
    return deduped


# ---------------------------------------------------------------------------
# HTML / JSON parsing
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


def _find_results_data(
    next_data: Dict[str, Any],
    player_id: str,
    season: Optional[int] = None,
    tour_code: str = "R",
) -> List[Dict[str, Any]]:
    """
    Navigate the dehydrated React Query state to find ``resultsData``.

    The page embeds a ``dehydratedState.queries`` array.  Each query has a
    ``queryKey`` like
        ["playerProfileResults", "40026", "season", {"tour": "R"}]
    or (when a specific season is selected)
        ["playerProfileResults", "40026", "season", {"season": "2026", "tour": "R"}]

    Returns a flat list of tournament entry dicts from ``resultsData[*].data``.
    """
    try:
        queries: List[Dict] = (
            next_data["props"]["pageProps"]["dehydratedState"]["queries"]
        )
    except (KeyError, TypeError):
        # dehydratedState or queries key missing — site structure may have changed.
        log_key = ("no_dehydrated", season)
        if log_key not in _DEBUG_LOGGED_SEASONS:
            _DEBUG_LOGGED_SEASONS.add(log_key)
            page_props_keys = list((next_data.get("props") or {}).get("pageProps") or {})
            print(
                f"[scorecard_scraper] DEBUG player={player_id} season={season}: "
                f"dehydratedState.queries not found in __NEXT_DATA__. "
                f"pageProps keys: {page_props_keys}",
                flush=True,
            )
        return []

    def _extract_tournament_rows(query: Dict) -> List[Dict[str, Any]]:
        data = (query.get("state") or {}).get("data") or {}
        results_data = data.get("resultsData") or []
        rows: List[Dict[str, Any]] = []
        for section in results_data:
            for entry in section.get("data") or []:
                if entry.get("tournamentId"):
                    rows.append(entry)
        return rows

    str_player_id = str(player_id)
    str_season = str(season) if season else None

    # First pass: prefer query matching player_id + season (if specified)
    for query in queries:
        key = query.get("queryKey") or []
        if not key or key[0] != "playerProfileResults":
            continue
        if len(key) < 2 or str(key[1]) != str_player_id:
            continue
        params: Dict = key[3] if len(key) > 3 else {}
        if str_season and str(params.get("season") or "") != str_season:
            continue
        if str(params.get("tour") or "") != tour_code:
            continue
        rows = _extract_tournament_rows(query)
        if rows:
            return rows

    # Second pass: any playerProfileResults for this player with resultsData
    for query in queries:
        key = query.get("queryKey") or []
        if not key or key[0] != "playerProfileResults":
            continue
        if len(key) < 2 or str(key[1]) != str_player_id:
            continue
        rows = _extract_tournament_rows(query)
        if rows:
            return rows

    # Third pass: any playerProfileResults query with resultsData
    for query in queries:
        key = query.get("queryKey") or []
        if key and key[0] == "playerProfileResults":
            rows = _extract_tournament_rows(query)
            if rows:
                return rows

    # Diagnostic: log what query keys are present so we can debug structure changes.
    log_key = ("no_results", season)
    if log_key not in _DEBUG_LOGGED_SEASONS:
        _DEBUG_LOGGED_SEASONS.add(log_key)
        all_keys = [q.get("queryKey", []) for q in queries if q.get("queryKey")]
        print(
            f"[scorecard_scraper] DEBUG player={player_id} season={season}: "
            f"no resultsData found. "
            f"Query keys present ({len(all_keys)}): "
            + ", ".join(str(k[:2]) for k in all_keys[:10]),
            flush=True,
        )

    return []


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


def _safe_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    s = str(v).strip()
    if not s or s in {"-", "E", "WD", "DQ", "MDF"}:
        return None
    try:
        return int(s)
    except (ValueError, TypeError):
        return None


def _season_from_tournament_id(tournament_id: str) -> int:
    """Extract the season year from a tournament ID like 'R2026006' → 2026."""
    try:
        return int(tournament_id[1:5])
    except (ValueError, IndexError):
        return 0


def _parse_tournament_entry(
    entry: Dict[str, Any],
    player_id: str,
    player_name: str,
) -> Optional[PlayerTournamentScorecard]:
    """Parse a single tournament entry from resultsData into a scorecard record."""
    tournament_id = str(entry.get("tournamentId") or "").strip()
    if not tournament_id:
        return None

    fields: List[str] = entry.get("fields") or []
    if len(fields) < 8:
        return None

    season = _season_from_tournament_id(tournament_id)
    tournament_date = str(fields[0]).strip() if len(fields) > 0 else ""
    tournament_name = str(fields[1]).strip() if len(fields) > 1 else ""
    position = str(fields[2]).strip() if len(fields) > 2 else ""
    r1 = _safe_int(fields[3]) if len(fields) > 3 else None
    r2 = _safe_int(fields[4]) if len(fields) > 4 else None
    r3 = _safe_int(fields[5]) if len(fields) > 5 else None
    r4 = _safe_int(fields[6]) if len(fields) > 6 else None
    total_strokes = _safe_int(fields[7]) if len(fields) > 7 else None
    to_par = str(fields[8]).strip() if len(fields) > 8 else None

    return PlayerTournamentScorecard(
        player_id=player_id,
        player_name=player_name,
        season=season,
        tournament_id=tournament_id,
        tournament_name=tournament_name,
        tournament_date=tournament_date,
        position=position,
        r1=r1,
        r2=r2,
        r3=r3,
        r4=r4,
        total_strokes=total_strokes,
        to_par=to_par,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def fetch_player_scorecard_history(
    player_id: str,
    player_name: str,
    season: Optional[int] = None,
    tour_code: str = "R",
    *,
    retries: int = 3,
    timeout: int = DEFAULT_TIMEOUT,
) -> List[PlayerTournamentScorecard]:
    """
    Fetch the tournament scorecard history for a single player.

    Args:
        player_id:   PGA Tour player ID (numeric string), e.g. ``"40026"``.
        player_name: Player display name, e.g. ``"Daniel Berger"``.
        season:      Season year (e.g. ``2025``).  ``None`` fetches the current
                     season from the default results page.
        tour_code:   Tour code, e.g. ``"R"`` (PGA Tour).
        retries:     Number of retry attempts on transient HTTP failures.
        timeout:     Request timeout in seconds.

    Returns:
        List of :class:`PlayerTournamentScorecard` (one per tournament).
        Returns an empty list if the player page has no results data.
    """
    candidate_urls = _candidate_results_urls(player_id, player_name)
    params: Dict[str, str] = {"tour": tour_code}
    if season is not None:
        params["season"] = str(season)

    backoff = 2
    last_exc: Optional[Exception] = None
    resp: Optional[requests.Response] = None

    for url in candidate_urls:
        for attempt in range(retries):
            try:
                resp = requests.get(
                    url,
                    headers=_browser_headers(),
                    params=params,
                    timeout=timeout,
                )
                if resp.status_code == 404:
                    break
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
        # success path
        if resp is not None and resp.status_code != 404:
            break

    if resp is not None and resp.status_code == 404:
        print(
            f"[scorecard_scraper] DEBUG 404 for player={player_id}; tried urls={candidate_urls}",
            flush=True,
        )
        return []

    if resp is None:
        raise RuntimeError(f"Exceeded {retries} retries. Last error: {last_exc}")

    try:
        next_data = _extract_next_data(resp.text)
    except (ValueError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            f"Failed to parse __NEXT_DATA__ for player {player_id}: {exc}"
        ) from exc

    raw_entries = _find_results_data(
        next_data,
        player_id=player_id,
        season=season,
        tour_code=tour_code,
    )

    results: List[PlayerTournamentScorecard] = []
    for entry in raw_entries:
        record = _parse_tournament_entry(entry, player_id, player_name)
        if record is not None:
            results.append(record)
    return results


def scorecard_history_to_records(
    rows: List[PlayerTournamentScorecard],
    *,
    run_ts: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Flatten PlayerTournamentScorecard list into BigQuery-ready row dicts.

    Produces one row per (player, tournament).
    """
    from datetime import datetime

    ts = run_ts or datetime.utcnow().isoformat()
    return [
        {
            "run_ts": ts,
            "ingested_at": ts,
            "season": r.season,
            "tournament_id": r.tournament_id,
            "tournament_name": r.tournament_name,
            "tournament_date": r.tournament_date,
            "player_id": r.player_id,
            "player_display_name": r.player_name,
            "position": r.position,
            "r1": r.r1,
            "r2": r.r2,
            "r3": r.r3,
            "r4": r.r4,
            "total_strokes": r.total_strokes,
            "to_par": r.to_par,
        }
        for r in rows
    ]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _cli() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch PGA Tour player tournament scorecard history from pgatour.com."
    )
    parser.add_argument(
        "--player-id",
        required=True,
        metavar="ID",
        help="PGA Tour player ID, e.g. 40026",
    )
    parser.add_argument(
        "--player-name",
        required=True,
        metavar="NAME",
        help='Player display name, e.g. "Daniel Berger"',
    )
    parser.add_argument(
        "--season",
        type=int,
        default=None,
        metavar="YEAR",
        help="Season year (default: current season from page)",
    )
    parser.add_argument(
        "--tour",
        default="R",
        metavar="TOUR_CODE",
        help="Tour code (default: R)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        dest="as_json",
        help="Output flat JSON records",
    )
    args = parser.parse_args()

    print(
        f"Fetching scorecard history: player_id={args.player_id} "
        f"name='{args.player_name}' season={args.season or 'current'}",
        file=sys.stderr,
    )

    rows = fetch_player_scorecard_history(
        player_id=args.player_id,
        player_name=args.player_name,
        season=args.season,
        tour_code=args.tour,
    )

    if not rows:
        print("No scorecard history found for this player / season.", file=sys.stderr)
        sys.exit(1)

    if args.as_json:
        print(json.dumps(scorecard_history_to_records(rows), indent=2, default=str))
        return

    print(f"\n  {rows[0].player_name} — {rows[0].season} ({args.tour})\n")
    print(
        f"  {'Date':12}  {'Tournament':35}  {'Pos':5}  "
        f"{'R1':>4}  {'R2':>4}  {'R3':>4}  {'R4':>4}  {'Total':>5}  {'ToPar':>6}"
    )
    print("  " + "-" * 95)
    for r in rows:
        print(
            f"  {r.tournament_date:12}  {r.tournament_name[:35]:35}  {r.position:5}  "
            f"{str(r.r1 or '-'):>4}  {str(r.r2 or '-'):>4}  "
            f"{str(r.r3 or '-'):>4}  {str(r.r4 or '-'):>4}  "
            f"{str(r.total_strokes or '-'):>5}  {str(r.to_par or '-'):>6}"
        )


if __name__ == "__main__":
    _cli()

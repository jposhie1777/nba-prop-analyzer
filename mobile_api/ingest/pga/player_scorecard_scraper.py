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
import os
import re
import sys
import time
import urllib.parse
import unicodedata
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests

RESULTS_PAGE_BASE = "https://www.pgatour.com/player"
RESULTS_PAGE_BASE_ALT = "https://pgatour.com/player"
DEFAULT_TIMEOUT = 30
GRAPHQL_ENDPOINT = "https://orchestrator.pgatour.com/graphql"
DEFAULT_API_KEY = os.getenv("PGA_TOUR_GQL_API_KEY", "da2-gsrx5bibzbb4njvhl7t37wqyl4")

# Tracks seasons for which we've already emitted a structure-change debug message,
# so we don't spam 214 identical lines per season.
_DEBUG_LOGGED_SEASONS: set = set()

# Tracks whether we've already logged the first GraphQL fallback attempt/result
# so we get one diagnostic line per season without spamming 214 lines.
_GQL_FALLBACK_LOGGED: set = set()


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
    course_name: Optional[str]
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

def _prepare_session() -> requests.Session:
    """Create a session and prime pgatour cookies to reduce bot 404 responses."""
    session = requests.Session()
    session.headers.update(_browser_headers())
    try:
        session.get("https://pgatour.com/", timeout=DEFAULT_TIMEOUT)
    except Exception:
        # Best-effort cookie priming only.
        pass
    return session


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
        f"{RESULTS_PAGE_BASE}/{player_id}/{display_slug}/results",
        f"{RESULTS_PAGE_BASE}/{player_id}/{ascii_slug}/results",
        f"https://www.pgatour.com/players/{player_id}/{ascii_slug}/results",
        f"{RESULTS_PAGE_BASE}/{player_id}/results",
        f"{RESULTS_PAGE_BASE_ALT}/{player_id}/{display_slug}/results",
        f"{RESULTS_PAGE_BASE_ALT}/{player_id}/{ascii_slug}/results",
        f"https://pgatour.com/players/{player_id}/{ascii_slug}/results",
        f"{RESULTS_PAGE_BASE_ALT}/{player_id}/results",
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
            section_course = section.get("courseName") or section.get("course")
            for entry in section.get("data") or []:
                if entry.get("tournamentId"):
                    if section_course and "__course_name" not in entry:
                        entry = dict(entry)
                        entry["__course_name"] = section_course
                    rows.append(entry)
        return rows

    str_player_id = str(player_id)
    str_season = str(season) if season else None

    def _query_params_from_key(key: List[Any]) -> Dict[str, Any]:
        """Return the most relevant params dict from a queryKey."""
        params: Dict[str, Any] = {}
        for part in key:
            if isinstance(part, dict):
                params.update(part)
        return params

    def _dedupe_tournaments(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        deduped: List[Dict[str, Any]] = []
        seen_tids = set()
        for row in rows:
            tid = str(row.get("tournamentId") or "").strip()
            if not tid or tid in seen_tids:
                continue
            seen_tids.add(tid)
            deduped.append(row)
        return deduped

    def _collect_rows(predicate) -> List[Dict[str, Any]]:
        collected: List[Dict[str, Any]] = []
        for query in queries:
            key = query.get("queryKey") or []
            if not predicate(key):
                continue
            collected.extend(_extract_tournament_rows(query))
        return _dedupe_tournaments(collected)

    # First pass: collect all queries matching player_id + season + tour.
    def _strict_match(key: List[Any]) -> bool:
        if not key or key[0] != "playerProfileResults":
            return False
        if len(key) < 2 or str(key[1]) != str_player_id:
            return False
        params = _query_params_from_key(key)
        if str_season and str(params.get("season") or "") != str_season:
            return False
        return str(params.get("tour") or "") == tour_code

    rows = _collect_rows(_strict_match)
    if rows:
        return rows

    # Second pass: player/tour match but season may be encoded differently.
    def _player_tour_match(key: List[Any]) -> bool:
        if not key or key[0] != "playerProfileResults":
            return False
        if len(key) < 2 or str(key[1]) != str_player_id:
            return False
        params = _query_params_from_key(key)
        return str(params.get("tour") or "") == tour_code

    rows = _collect_rows(_player_tour_match)
    if rows:
        return rows

    # Third pass: any playerProfileResults for this player.
    def _player_only_match(key: List[Any]) -> bool:
        return bool(key) and key[0] == "playerProfileResults" and len(key) > 1 and str(key[1]) == str_player_id

    rows = _collect_rows(_player_only_match)
    if rows:
        return rows

    # Fourth pass: any playerProfileResults query with resultsData.
    rows = _collect_rows(lambda key: bool(key) and key[0] == "playerProfileResults")
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


def _extract_course_name(entry: Dict[str, Any]) -> Optional[str]:
    """Best-effort extraction of course name from resultsData entry payload."""
    direct_keys = ("courseName", "course", "hostCourse", "venue", "course_name")
    for key in direct_keys:
        value = entry.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, dict):
            for nested_key in ("name", "displayName", "courseName"):
                nested = value.get(nested_key)
                if isinstance(nested, str) and nested.strip():
                    return nested.strip()

    section_course = entry.get("__course_name")
    if isinstance(section_course, str) and section_course.strip():
        return section_course.strip()

    return None


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
    course_name = _extract_course_name(entry)
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
        course_name=course_name,
        position=position,
        r1=r1,
        r2=r2,
        r3=r3,
        r4=r4,
        total_strokes=total_strokes,
        to_par=to_par,
    )


# ---------------------------------------------------------------------------
# GraphQL fallback for historical seasons
# ---------------------------------------------------------------------------

# Cached introspection result: maps field_name -> list of arg names.
# None = not yet probed.  Empty dict = probed but nothing found.
_GQL_FIELD_CACHE: Optional[Dict[str, List[str]]] = None


def _gql_headers() -> Dict[str, str]:
    return {
        "Content-Type": "application/json",
        "x-api-key": DEFAULT_API_KEY,
        "x-pgat-platform": "web",
        "Referer": "https://www.pgatour.com/",
        "Origin": "https://www.pgatour.com",
    }


def _introspect_player_result_fields(timeout: int) -> Dict[str, List[str]]:
    """
    Query the PGA Tour GraphQL schema and return every field whose name
    contains both 'player' and any of {'result','history','career','profile'},
    PLUS every field that accepts both a playerId-style arg and a season/year arg.

    Returns {field_name: [arg_name, ...]} for each candidate.
    Logs everything found so the caller can identify the correct field.
    """
    introspect_q = """
    {
      __schema {
        queryType {
          fields {
            name
            args { name type { name kind } }
          }
        }
      }
    }
    """
    try:
        resp = requests.post(
            GRAPHQL_ENDPOINT,
            headers=_gql_headers(),
            json={"query": introspect_q},
            timeout=timeout,
        )
        resp.raise_for_status()
        schema_data = resp.json()
        all_fields = schema_data["data"]["__schema"]["queryType"]["fields"]
    except Exception as exc:
        print(
            f"[scorecard_scraper] GQL schema introspection failed: "
            f"{type(exc).__name__}: {exc}",
            flush=True,
        )
        return {}

    result_keywords = {"result", "history", "career", "profile", "season"}
    candidates: Dict[str, List[str]] = {}

    for f in all_fields:
        name_lower = f["name"].lower()
        arg_names = [a["name"] for a in f.get("args", [])]
        arg_lower = {a.lower() for a in arg_names}

        # Include if name has "player" + any result-ish keyword
        if "player" in name_lower and any(k in name_lower for k in result_keywords):
            candidates[f["name"]] = arg_names
            continue

        # Include if field accepts a player ID arg + a season/year arg
        has_player_arg = any("player" in a or a in {"id"} for a in arg_lower)
        has_season_arg = any(a in {"season", "year"} for a in arg_lower)
        if has_player_arg and has_season_arg:
            candidates[f["name"]] = arg_names

    # Always log what we found — one-time diagnostic.
    all_player_names = sorted(
        f["name"] for f in all_fields if "player" in f["name"].lower()
    )
    print(
        f"[scorecard_scraper] GQL schema probe — "
        f"all 'player' fields ({len(all_player_names)}): {all_player_names}",
        flush=True,
    )
    print(
        f"[scorecard_scraper] GQL schema probe — "
        f"result/history/season candidates: "
        + ", ".join(
            f"{k}({', '.join(v)})" for k, v in candidates.items()
        ) if candidates else
        f"[scorecard_scraper] GQL schema probe — no result/history candidates found.",
        flush=True,
    )
    return candidates


def _build_results_query(field_name: str, arg_names: List[str]) -> tuple:
    """
    Build a GraphQL query string and variables dict for the given field,
    trying common arg name conventions for playerId, season, and tour.
    Returns (query_str, variables_dict).
    """
    arg_lower = {a.lower(): a for a in arg_names}

    # Map our logical params to whatever the field actually uses.
    player_arg = (
        arg_lower.get("playerid")
        or arg_lower.get("playerid")  # same, but handles capitalisation
        or arg_lower.get("id")
        or "playerId"
    )
    season_arg = arg_lower.get("season") or arg_lower.get("year") or "season"
    tour_arg = (
        arg_lower.get("tour")
        or arg_lower.get("tourcode")
        or arg_lower.get("tourid")
        or "tour"
    )

    query_str = f"""
query _HistoricalResults(${player_arg}: ID!, ${season_arg}: String, ${tour_arg}: String) {{
  {field_name}({player_arg}: ${player_arg}, {season_arg}: ${season_arg}, {tour_arg}: ${tour_arg}) {{
    resultsData {{
      courseName
      course
      data {{
        tournamentId
        fields
      }}
    }}
  }}
}}
"""
    return query_str, player_arg, season_arg, tour_arg


def _fetch_player_results_via_graphql(
    player_id: str,
    season: int,
    tour_code: str = "R",
    *,
    timeout: int = DEFAULT_TIMEOUT,
) -> List[Dict[str, Any]]:
    """
    Fetch player tournament results directly from the PGA Tour GraphQL API.

    Used as a fallback when the HTML page's __NEXT_DATA__ only contains current-
    season data (pgatour.com SSR does not hydrate historical seasons server-side;
    the browser fetches them client-side via this same GraphQL endpoint).

    Returns raw tournament entry dicts in the same format as _find_results_data,
    ready for _parse_tournament_entry.  Returns [] on any error.
    """
    global _GQL_FIELD_CACHE

    _log_once = season not in _GQL_FALLBACK_LOGGED

    # Introspect schema once to find the real field name.
    if _GQL_FIELD_CACHE is None:
        _GQL_FIELD_CACHE = _introspect_player_result_fields(timeout)

    if not _GQL_FIELD_CACHE:
        if _log_once:
            _GQL_FALLBACK_LOGGED.add(season)
            print(
                f"[scorecard_scraper] GQL fallback season={season}: "
                f"no suitable query field found in schema — skipping GQL fallback.",
                flush=True,
            )
        return []

    headers = _gql_headers()

    for field_name, arg_names in _GQL_FIELD_CACHE.items():
        query_str, player_arg, season_arg, tour_arg = _build_results_query(
            field_name, arg_names
        )
        variables = {
            player_arg: str(player_id),
            season_arg: str(season),
            tour_arg: tour_code,
        }
        try:
            resp = requests.post(
                GRAPHQL_ENDPOINT,
                headers=headers,
                json={"query": query_str, "variables": variables},
                timeout=timeout,
            )
            resp.raise_for_status()
            data = resp.json()

            if "errors" in data:
                if _log_once:
                    _GQL_FALLBACK_LOGGED.add(season)
                    print(
                        f"[scorecard_scraper] GQL fallback season={season} "
                        f"field={field_name}: errors={data['errors'][:1]}",
                        flush=True,
                    )
                continue  # try next candidate

            field_data = (data.get("data") or {}).get(field_name) or {}
            results_data = field_data.get("resultsData") or []

            if _log_once:
                _GQL_FALLBACK_LOGGED.add(season)
                print(
                    f"[scorecard_scraper] GQL fallback season={season} "
                    f"field={field_name}: "
                    f"resultsData sections={len(results_data)}",
                    flush=True,
                )

            rows: List[Dict[str, Any]] = []
            for section in results_data:
                section_course = section.get("courseName") or section.get("course")
                for entry in section.get("data") or []:
                    if entry.get("tournamentId"):
                        if section_course and "__course_name" not in entry:
                            entry = dict(entry)
                            entry["__course_name"] = section_course
                        rows.append(entry)
            if rows:
                return rows

        except Exception as exc:
            if _log_once:
                _GQL_FALLBACK_LOGGED.add(season)
                print(
                    f"[scorecard_scraper] GQL fallback season={season} "
                    f"field={field_name}: {type(exc).__name__}: {exc}",
                    flush=True,
                )

    return []


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
    session = _prepare_session()
    params: Dict[str, str] = {"tour": tour_code}
    if season is not None:
        params["season"] = str(season)

    backoff = 2
    last_exc: Optional[Exception] = None
    resp: Optional[requests.Response] = None

    for url in candidate_urls:
        for attempt in range(retries):
            try:
                resp = session.get(
                    url,
                    params=params,
                    timeout=timeout,
                )
                if resp.status_code in (403, 404):
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
        if resp is not None and resp.status_code not in (403, 404):
            break

    if resp is not None and resp.status_code in (403, 404):
        print(
            f"[scorecard_scraper] DEBUG {resp.status_code} for player={player_id}; tried urls={candidate_urls}; params={params}",
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
        if record is None:
            continue
        results.append(record)

    if season is not None:
        season_rows = [r for r in results if int(r.season or 0) == int(season)]
        if season_rows:
            return season_rows

        # HTML page returned wrong-season data (pgatour.com SSR always embeds the
        # current season regardless of ?season= URL param).  Fall back to the PGA
        # Tour GraphQL API, which is what the browser fetches client-side when the
        # user selects a historical season from the dropdown.
        gql_entries = _fetch_player_results_via_graphql(
            player_id, season, tour_code, timeout=timeout
        )
        if gql_entries:
            gql_results: List[PlayerTournamentScorecard] = []
            for entry in gql_entries:
                record = _parse_tournament_entry(entry, player_id, player_name)
                if record is not None:
                    gql_results.append(record)
            season_rows = [r for r in gql_results if int(r.season or 0) == int(season)]
            if season_rows:
                return season_rows

        if results:
            seasons_found = sorted({int(r.season or 0) for r in results if r.season})
            print(
                f"[scorecard_scraper] DEBUG player={player_id} requested season={season} "
                f"but found seasons={seasons_found}; returning 0 rows for this season.",
                flush=True,
            )
        return []

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
            "course_name": r.course_name,
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
        f"  {'Date':12}  {'Tournament':28}  {'Course':24}  {'Pos':5}  "
        f"{'R1':>4}  {'R2':>4}  {'R3':>4}  {'R4':>4}  {'Total':>5}  {'ToPar':>6}"
    )
    print("  " + "-" * 124)
    for r in rows:
        print(
            f"  {r.tournament_date:12}  {r.tournament_name[:28]:28}  {(r.course_name or '-')[:24]:24}  {r.position:5}  "
            f"{str(r.r1 or '-'):>4}  {str(r.r2 or '-'):>4}  "
            f"{str(r.r3 or '-'):>4}  {str(r.r4 or '-'):>4}  "
            f"{str(r.total_strokes or '-'):>5}  {str(r.to_par or '-'):>6}"
        )


if __name__ == "__main__":
    _cli()

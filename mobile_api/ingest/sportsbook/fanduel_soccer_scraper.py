"""
FanDuel Soccer Market Scraper — v4

Architecture:
  Phase 1 — Camoufox loads the competition page (1 browser session):
    - Captures sbapi nav response  → moneyline market IDs for ALL upcoming games
    - Captures getMarketPrices     → additional market IDs
    - Captures request headers     → x-px-context token
    - Builds full event map        → event_id, home/away team, kickoff time

  Phase 2 — Direct API calls for near-term events (next 8 days):
    - Calls event-page API for each event × each tab (popular, goals, half,
      team-props, cards-fouls, penalties) using the captured px token
    - Collects all market IDs available across every tab

  Phase 3 — getMarketPrices batch POST:
    - POSTs all discovered market IDs to getMarketPrices
    - Classifies markets, builds deep links, writes to BigQuery

  Result:
    - Moneyline for ALL upcoming games on the competition page
    - Full market set (all tabs) for games within the next 8 days

Usage:
  python -m mobile_api.ingest.sportsbook.fanduel_soccer_scraper --league MLS --dry-run
  python -m mobile_api.ingest.sportsbook.fanduel_soccer_scraper --league EPL --scrape-only
  python -m mobile_api.ingest.sportsbook.fanduel_soccer_scraper --league MLS --load-only
  python -m mobile_api.ingest.sportsbook.fanduel_soccer_scraper --league EPL --discover
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urlencode

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DATASET = "sportsbook"
TABLE = "raw_fanduel_soccer_markets"
ARTIFACT_PATTERN = "/tmp/fanduel_soccer_{league}_rows.ndjson"

FD_BASE_URL = "https://sportsbook.fanduel.com"

FD_MARKET_PRICES_URL = (
    "https://smp.ia.sportsbook.fanduel.com"
    "/api/sports/fixedodds/readonly/v1/getMarketPrices?priceHistory=1"
)

FD_API_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Origin": "https://sportsbook.fanduel.com",
    "Referer": "https://sportsbook.fanduel.com/",
    "X-Application": "FhMFpcPWXMeyZxOx",
}

MARKET_PRICES_BATCH_SIZE = 50

# Capture patterns:
#   "sbapi"           → api.sportsbook.fanduel.com/sbapi/competition-page  (nav + attachments)
#                     → api.sportsbook.fanduel.com/sbapi/event-page
#   "getMarketPrices" → smp.*.sportsbook.fanduel.com/.../getMarketPrices   (live odds)
CAPTURE_PATTERNS = [
    "sbapi",
    "getMarketPrices",
]

# Event-page API — called directly (with px token) for near-term games.
# Tab names match what FanDuel passes as the `tab=` query parameter.
FD_EVENT_PAGE_URL = (
    "https://api.sportsbook.fanduel.com/sbapi/event-page"
    "?_ak=FhMFpcPWXMeyZxOx&eventId={event_id}&tab={tab}"
    "&useCombinedTouchdownsVirtualMarket=true&useQuickBets=true"
)

# All tabs to fetch per near-term event. Each call returns a different
# slice of attachments.markets, so we union the results.
EVENT_PAGE_TABS = ["popular", "goals", "half", "team-props", "cards-fouls", "penalties"]

# Events kicking off within this many days get the full per-tab treatment.
NEAR_TERM_DAYS = 8

LEAGUE_CONFIG: Dict[str, Dict[str, Any]] = {
    "MLS": {
        "url": f"{FD_BASE_URL}/soccer/us-mls",
        "competition_id": "141",
    },
    "EPL": {
        "url": f"{FD_BASE_URL}/soccer/english-premier-league",
        "competition_id": "10932509",
    },
}

# ---------------------------------------------------------------------------
# Soccer market classification
# marketType string from FanDuel nav → broad bucket used in BigQuery
# ---------------------------------------------------------------------------

MARKET_TYPE_MAP: Dict[str, str] = {
    # Moneyline / match result
    "WIN-DRAW-WIN": "moneyline",
    "FULL_TIME_RESULT_-_2_UP": "moneyline",
    "DRAW_NO_BET": "moneyline",
    "DOUBLE_CHANCE": "moneyline",
    # Handicap / spread
    "HANDICAP_BETTING": "handicap",
    "ALTERNATIVE_HANDICAPS": "handicap",
    "FIRST_HALF_HANDICAP": "handicap",
    "HOME_TEAM_-1.5_GOALS": "handicap",
    "HOME_TEAM_-2.5_GOALS": "handicap",
    "HOME_TEAM_-3.5_GOALS": "handicap",
    "AWAY_TEAM_-1.5_GOALS": "handicap",
    "AWAY_TEAM_-2.5_GOALS": "handicap",
    # Match totals
    "OVER_UNDER_05": "totals",
    "OVER_UNDER_15": "totals",
    "OVER_UNDER_25": "totals",
    "OVER_UNDER_35": "totals",
    "OVER_UNDER_45": "totals",
    "OVER_UNDER_55": "totals",
    "OVER_UNDER_65": "totals",
    "OVER_UNDER_75": "totals",
    "MULTIGOL_-_MATCH": "totals",
    "MULTIGOL_-_2ND_HALF": "totals",
    # Both teams to score
    "BOTH_TEAMS_TO_SCORE": "btts",
    "BOTH_TEAMS_TO_SCORE_IN_THE_FIRST_HALF": "btts",
    "BOTH_TEAMS_TO_SCORE_&_O/U_2.5_GOALS": "btts",
    "RESULT_&_BOTH_TO_SCORE": "btts",
    # Correct score
    "CORRECT_SCORE": "correct_score",
    "CORRECT_SCORE_COMBINATIONS": "correct_score",
    "HALF-TIME_CORRECT_SCORE": "correct_score",
    # Half-time markets
    "HALF-TIME_RESULT": "halftime",
    "HALF-TIME/FULL-TIME": "halftime",
    "TO_WIN_EITHER_HALF": "halftime",
    "TO_WIN_BOTH_HALVES": "halftime",
    "HALF_WITH_MOST_GOALS": "halftime",
    "TO_BE_WINNING_AT_HT_OR_FT": "halftime",
    # Half-time totals
    "1ST_HALF_OVER/UNDER_0.5_GOALS": "halftime_totals",
    "1ST_HALF_OVER/UNDER_1.5_GOALS": "halftime_totals",
    "1ST_HALF_OVER/UNDER_2.5_GOALS": "halftime_totals",
    "1ST_HALF_OVER/UNDER_3.5_GOALS": "halftime_totals",
    "1ST_HALF_OVER/UNDER_4.5_GOALS": "halftime_totals",
    "MULTIGOL_-_1ST_HALF": "halftime_totals",
    # Team totals
    "HOME_TEAM_OVER/UNDER_0.5": "team_totals",
    "HOME_TEAM_OVER/UNDER_1.5": "team_totals",
    "HOME_TEAM_OVER/UNDER_2.5": "team_totals",
    "HOME_TEAM_OVER/UNDER_3.5": "team_totals",
    "HOME_TEAM_OVER/UNDER_4.5": "team_totals",
    "AWAY_TEAM_OVER/UNDER_0.5": "team_totals",
    "AWAY_TEAM_OVER/UNDER_1.5": "team_totals",
    "AWAY_TEAM_OVER/UNDER_2.5": "team_totals",
    "AWAY_TEAM_OVER/UNDER_3.5": "team_totals",
    "MULTIGOL_-_HOME": "team_totals",
    "MULTIGOL_-_AWAY": "team_totals",
    "MULTIGOL_-_HOME_/_AWAY": "team_totals",
    "NUMBER_OF_TEAM_GOALS": "team_totals",
    "TEAM_FIRST_HALF_GOALS": "team_totals",
    "HOME_TEAM_FIRST_HALF_OVER/UNDER_0.5": "team_totals",
    "HOME_TEAM_FIRST_HALF_OVER/UNDER_1.5": "team_totals",
    "HOME_TEAM_FIRST_HALF_OVER/UNDER_2.5": "team_totals",
    "AWAY_TEAM_FIRST_HALF_OVER/UNDER_0.5": "team_totals",
    "AWAY_TEAM_FIRST_HALF_OVER/UNDER_1.5": "team_totals",
    "AWAY_TEAM_FIRST_HALF_OVER/UNDER_2.5": "team_totals",
    # Team props
    "TEAM_TO_SCORE_THE_FIRST_GOAL": "team_props",
    "TO_SCORE_IN_BOTH_HALVES": "team_props",
    "A_GOAL_SCORED_IN_BOTH_HALVES": "team_props",
    "LEAD_AT_10-20-30-60_MINUTES": "team_props",
    "WINNING_MARGIN": "team_props",
    # Combo (result + totals)
    "WDW_&_O/U_1.5_GOALS": "combo",
    "WDW_&_O/U_2.5_GOALS": "combo",
    "WDW_&_O/U_3.5_GOALS": "combo",
    "WDW_&_O/U_4.5_GOALS": "combo",
    # Cards / fouls / penalties
    "PENALTY_AWARDED?": "cards_fouls",
    # Player props (appear closer to kickoff)
    "TO_SCORE": "player_props",
    "ANYTIME_ASSIST": "player_props",
    # Futures / outrights
    "OUTRIGHT_BETTING": "futures",
    "TOP_GOALSCORER": "futures",
    "TOP_4_FINISH": "futures",
    "TOP_5_FINISH": "futures",
    "TOP_6_FINISH": "futures",
    "TOP_HALF_FINISH": "futures",
    "TO_BE_RELEGATED": "futures",
    "AVOID_RELEGATION": "futures",
    "FINISH_BOTTOM": "futures",
    "MULTIPLE_TROPHIES": "futures",
    "BETTING_WITHOUT": "futures",
}


def _classify_market(market_type_raw: str) -> str:
    return MARKET_TYPE_MAP.get(market_type_raw, "other")


# ---------------------------------------------------------------------------
# Camoufox proxy helpers
# ---------------------------------------------------------------------------

def _get_camoufox_url() -> str:
    url = os.environ.get("CAMOUFOX_SERVICE_URL", "").rstrip("/")
    if not url:
        raise RuntimeError("CAMOUFOX_SERVICE_URL env var is not set")
    return url


def _get_camoufox_token() -> str:
    return os.environ.get("CAMOUFOX_TOKEN", "")


def _call_proxy(payload: Dict[str, Any]) -> Dict[str, Any]:
    service_url = _get_camoufox_url()
    token = _get_camoufox_token()
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    resp = requests.post(
        f"{service_url}/fetch",
        json=payload,
        headers=headers,
        timeout=150,
    )
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# Nav response parsing
# ---------------------------------------------------------------------------

def _extract_market_ids_from_nav(data: Dict[str, Any]) -> Set[str]:
    """Return all market IDs from a competition-page / event-page nav response."""
    market_ids: Set[str] = set()
    for market_id, market in data.get("attachments", {}).get("markets", {}).items():
        if not isinstance(market, dict):
            continue
        market_ids.add(str(market_id))
        for assoc in market.get("associatedMarkets", []):
            if isinstance(assoc, dict) and assoc.get("marketId"):
                market_ids.add(str(assoc["marketId"]))
    return market_ids


def _extract_market_names_from_nav(data: Dict[str, Any]) -> Dict[str, str]:
    """Build market_id → marketName from nav attachments."""
    names: Dict[str, str] = {}
    for market_id, market in data.get("attachments", {}).get("markets", {}).items():
        if not isinstance(market, dict):
            continue
        name = market.get("marketName") or market.get("name") or ""
        if name:
            names[str(market_id)] = name
    return names


def _extract_market_type_raw_from_nav(data: Dict[str, Any]) -> Dict[str, str]:
    """Build market_id → marketType (raw FanDuel code) from nav attachments."""
    types: Dict[str, str] = {}
    for market_id, market in data.get("attachments", {}).get("markets", {}).items():
        if not isinstance(market, dict):
            continue
        mt = market.get("marketType") or ""
        if mt:
            types[str(market_id)] = mt
    return types


def _extract_market_to_event_map(data: Dict[str, Any]) -> Dict[str, str]:
    """Build market_id → event_id from nav attachments."""
    m2e: Dict[str, str] = {}
    for market_id, market in data.get("attachments", {}).get("markets", {}).items():
        if not isinstance(market, dict):
            continue
        event_id = str(market.get("eventId") or "")
        if event_id:
            m2e[str(market_id)] = event_id
    return m2e


def _extract_events_from_nav(data: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    Build event_id → {name, home_team, away_team, event_start} from nav attachments.
    FanDuel event names use "Home v Away" format.
    """
    events: Dict[str, Dict[str, Any]] = {}
    for event_id, ev in data.get("attachments", {}).get("events", {}).items():
        if not isinstance(ev, dict):
            continue
        name = ev.get("name") or ""
        home_team = ""
        away_team = ""
        if " v " in name:
            parts = name.split(" v ", 1)
            home_team = parts[0].strip()
            away_team = parts[1].strip()
        start_raw = ev.get("openDate") or ev.get("startTime") or ""
        event_start = None
        if start_raw:
            try:
                event_start = datetime.fromisoformat(
                    start_raw.replace("Z", "+00:00")
                ).strftime("%Y-%m-%dT%H:%M:%S")
            except Exception:
                event_start = start_raw
        events[str(event_id)] = {
            "name": name,
            "home_team": home_team or None,
            "away_team": away_team or None,
            "event_start": event_start,
        }
    return events


def _try_parse_json(body: Any) -> Optional[Dict[str, Any]]:
    if isinstance(body, dict):
        return body
    if isinstance(body, str):
        s = body.strip()
        if s.startswith("{"):
            try:
                parsed = json.loads(s)
                if isinstance(parsed, dict):
                    return parsed
            except json.JSONDecodeError:
                pass
    return None


def _is_nav_response(data: Dict[str, Any]) -> bool:
    """True if this looks like a competition-page / event-page nav payload."""
    return (
        isinstance(data, dict)
        and ("layout" in data or "attachments" in data)
        and bool(data.get("attachments", {}).get("markets"))
    )


# ---------------------------------------------------------------------------
# Main page load + capture extraction
# ---------------------------------------------------------------------------

def _scrape_competition_page(
    league: str,
) -> Tuple[Set[str], Dict[str, Dict[str, Any]], Dict[str, str], Dict[str, str], Dict[str, str], str]:
    """
    Load the competition page via Camoufox and extract everything from captures.

    Returns:
        market_ids        — all discovered FanDuel market IDs
        event_map         — event_id → {name, home_team, away_team, event_start}
        market_name_map   — market_id → display name
        market_type_map   — market_id → raw marketType code
        market_to_event   — market_id → event_id
        px_context        — x-px-context token for getMarketPrices auth
    """
    cfg = LEAGUE_CONFIG[league.upper()]
    page_url = cfg["url"]

    market_ids: Set[str] = set()
    event_map: Dict[str, Dict[str, Any]] = {}
    market_name_map: Dict[str, str] = {}
    market_type_map: Dict[str, str] = {}
    market_to_event: Dict[str, str] = {}
    px_context = ""

    logger.info("Loading via Camoufox: %s", page_url)
    result = _call_proxy({
        "url": page_url,
        "prime_url": FD_BASE_URL,
        "capture_patterns": CAPTURE_PATTERNS,
        "wait_ms": 30000,
        "timeout_ms": 120000,
    })

    page_status = result.get("status", 0)
    captured = result.get("captured_requests", [])
    logger.info("page_status=%s  captured=%d", page_status, len(captured))

    for i, cap in enumerate(captured):
        cap_url = cap.get("url", "")
        cap_body = cap.get("body")
        req_headers = cap.get("request_headers") or cap.get("requestHeaders") or {}

        body_type = type(cap_body).__name__
        if isinstance(cap_body, str):
            preview = cap_body[:100]
        elif isinstance(cap_body, dict):
            preview = str(list(cap_body.keys())[:6])
        elif isinstance(cap_body, list):
            preview = f"list[{len(cap_body)}]"
        else:
            preview = repr(cap_body)[:60]
        logger.info("  [%02d] %s  body=%s  preview=%r", i, cap_url[:120], body_type, preview)

        # px token from request headers
        if not px_context and isinstance(req_headers, dict):
            for k, v in req_headers.items():
                if k.lower() == "x-px-context":
                    px_context = str(v)
                    logger.info("  → x-px-context captured (%d chars)", len(px_context))
                    break

        # sbapi (competition-page / event-page) → nav JSON with layout + attachments
        if "sbapi" in cap_url:
            nav_data = _try_parse_json(cap_body)
            if nav_data and _is_nav_response(nav_data):
                nav_ids = _extract_market_ids_from_nav(nav_data)
                nav_names = _extract_market_names_from_nav(nav_data)
                nav_types = _extract_market_type_raw_from_nav(nav_data)
                nav_m2e = _extract_market_to_event_map(nav_data)
                nav_events = _extract_events_from_nav(nav_data)
                market_ids.update(nav_ids)
                market_name_map.update(nav_names)
                market_type_map.update(nav_types)
                market_to_event.update(nav_m2e)
                event_map.update(nav_events)
                logger.info(
                    "  → sbapi nav: %d marketIds, %d events, %d market names",
                    len(nav_ids), len(nav_events), len(nav_names),
                )

        # getMarketPrices response — extract any market IDs not already in nav
        if "getMarketPrices" in cap_url:
            if isinstance(cap_body, list):
                ids = [
                    str(mkt["marketId"])
                    for mkt in cap_body
                    if isinstance(mkt, dict) and mkt.get("marketId")
                ]
                market_ids.update(ids)
                logger.info("  → getMarketPrices response: %d marketIds", len(ids))
            else:
                body_data = _try_parse_json(cap_body)
                if isinstance(body_data, dict) and "marketIds" in body_data:
                    ids = [str(m) for m in body_data["marketIds"] if m]
                    market_ids.update(ids)
                    logger.info("  → getMarketPrices POST body: %d marketIds", len(ids))

    if not market_ids:
        logger.warning(
            "0 marketIds from %d captured requests. "
            "Run --discover to see all XHRs fired by the page.",
            len(captured),
        )

    logger.info(
        "Result: %d marketIds, %d events, %d market names, px=%s",
        len(market_ids), len(event_map), len(market_name_map), "YES" if px_context else "NO",
    )
    return market_ids, event_map, market_name_map, market_type_map, market_to_event, px_context


# ---------------------------------------------------------------------------
# Odds helpers
# ---------------------------------------------------------------------------

def _american_from_decimal(decimal: float) -> str:
    if decimal <= 1.0:
        return "N/A"
    if decimal >= 2.0:
        return f"+{int(round((decimal - 1) * 100))}"
    return str(int(round(-100 / (decimal - 1))))


def _build_deep_link(market_id: str, selection_id: str) -> str:
    params = urlencode([("marketId[]", market_id), ("selectionId[]", selection_id)])
    return f"fanduelsportsbook://launch?deepLink=addToBetslip%3F{params}"


# ---------------------------------------------------------------------------
# Parse getMarketPrices response
# ---------------------------------------------------------------------------

def _parse_market_prices_response(
    body: List[Any],
    scraped_at: str,
    league: str = "",
    event_map: Optional[Dict[str, Dict[str, Any]]] = None,
    market_name_map: Optional[Dict[str, str]] = None,
    market_type_map: Optional[Dict[str, str]] = None,
    market_to_event: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not isinstance(body, list):
        return rows

    event_map = event_map or {}
    market_name_map = market_name_map or {}
    market_type_map = market_type_map or {}
    market_to_event = market_to_event or {}
    raw_str = json.dumps(body)[:32000]

    for market in body:
        if not isinstance(market, dict):
            continue

        market_id = str(market.get("marketId", ""))
        if not market_id:
            continue

        turn_in_play = bool(market.get("turnInPlayEnabled", True))
        inplay = bool(market.get("inplay", False))
        market_status = market.get("marketStatus", "")

        market_name = market_name_map.get(market_id) or market.get("marketName") or ""
        market_type_raw = market_type_map.get(market_id) or ""
        market_type = _classify_market(market_type_raw)

        event_id = market_to_event.get(market_id, "")
        ev_info = event_map.get(event_id, {})
        home_team = ev_info.get("home_team")
        away_team = ev_info.get("away_team")
        event_start = ev_info.get("event_start")

        runners = market.get("runnerDetails", [])
        if not runners:
            continue

        for runner in runners:
            if not isinstance(runner, dict):
                continue

            selection_id = str(runner.get("selectionId", ""))
            selection_name = runner.get("runnerName") or ""
            runner_status = runner.get("runnerStatus", "")
            handicap = runner.get("handicap")
            try:
                handicap_f = float(handicap) if handicap is not None else None
            except Exception:
                handicap_f = None

            win_odds = runner.get("winRunnerOdds") or {}
            true_odds = win_odds.get("trueOdds") or {}
            dec_obj = true_odds.get("decimalOdds") or {}
            am_obj = win_odds.get("americanDisplayOdds") or {}

            odds_dec = None
            odds_am = None
            try:
                dec_val = dec_obj.get("decimalOdds")
                if dec_val is not None:
                    odds_dec = float(dec_val)
            except Exception:
                pass
            try:
                am_val = am_obj.get("americanOdds")
                if am_val is not None:
                    am_int = int(am_val)
                    odds_am = f"+{am_int}" if am_int >= 0 else str(am_int)
            except Exception:
                pass
            if odds_dec is not None and odds_am is None:
                odds_am = _american_from_decimal(odds_dec)

            deep_link = (
                _build_deep_link(market_id, selection_id)
                if market_id and selection_id else None
            )

            rows.append({
                "scraped_at": scraped_at,
                "source": "getMarketPrices",
                "league": league or None,
                "event_id": event_id or None,
                "home_team": home_team,
                "away_team": away_team,
                "event_start": event_start,
                "market_id": market_id,
                "market_name": market_name or None,
                "market_type": market_type,
                "market_type_raw": market_type_raw or None,
                "market_status": market_status or None,
                "turn_in_play": turn_in_play,
                "inplay": inplay,
                "selection_id": selection_id or None,
                "selection_name": selection_name or None,
                "runner_status": runner_status or None,
                "handicap": handicap_f,
                "odds_decimal": odds_dec,
                "odds_american": odds_am,
                "deep_link": deep_link,
                "raw_response": raw_str,
            })

    return rows


# ---------------------------------------------------------------------------
# getMarketPrices batched POST
# ---------------------------------------------------------------------------

def _fetch_market_prices(
    market_ids: List[str],
    scraped_at: str,
    league: str = "",
    event_map: Optional[Dict[str, Dict[str, Any]]] = None,
    market_name_map: Optional[Dict[str, str]] = None,
    market_type_map: Optional[Dict[str, str]] = None,
    market_to_event: Optional[Dict[str, str]] = None,
    px_context: str = "",
) -> List[Dict[str, Any]]:
    all_rows: List[Dict[str, Any]] = []
    batches = [
        market_ids[i:i + MARKET_PRICES_BATCH_SIZE]
        for i in range(0, len(market_ids), MARKET_PRICES_BATCH_SIZE)
    ]

    headers = {**FD_API_HEADERS}
    if px_context:
        headers["x-px-context"] = px_context
        logger.info("Using x-px-context token (%d chars)", len(px_context))
    else:
        logger.warning("No x-px-context — PerimeterX may block requests")

    for i, batch in enumerate(batches):
        logger.info("getMarketPrices batch %d/%d (%d ids)", i + 1, len(batches), len(batch))
        try:
            resp = requests.post(
                FD_MARKET_PRICES_URL,
                json={"marketIds": batch},
                headers=headers,
                timeout=30,
            )
            resp.raise_for_status()
            body = resp.json()
            rows = _parse_market_prices_response(
                body,
                scraped_at=scraped_at,
                league=league,
                event_map=event_map,
                market_name_map=market_name_map,
                market_type_map=market_type_map,
                market_to_event=market_to_event,
            )
            logger.info("  → %d rows", len(rows))
            all_rows.extend(rows)
        except Exception as exc:
            logger.warning("Batch %d failed: %s", i + 1, exc)
        if i < len(batches) - 1:
            time.sleep(0.5)

    return all_rows


# ---------------------------------------------------------------------------
# Phase 2 — near-term event market fetching
# ---------------------------------------------------------------------------

def _identify_near_term_events(
    event_map: Dict[str, Dict[str, Any]],
    days: int = NEAR_TERM_DAYS,
) -> List[str]:
    """Return event IDs whose kickoff falls within the next `days` days (UTC)."""
    now = datetime.now(timezone.utc)
    cutoff = now + timedelta(days=days)
    near_term: List[str] = []
    for event_id, ev in event_map.items():
        start_raw = ev.get("event_start")
        if not start_raw:
            continue
        try:
            # event_start is stored as "%Y-%m-%dT%H:%M:%S" (UTC, no tz suffix)
            start_dt = datetime.fromisoformat(start_raw).replace(tzinfo=timezone.utc)
            if now <= start_dt <= cutoff:
                near_term.append(event_id)
        except Exception:
            pass
    near_term.sort()
    return near_term


def _fetch_event_page_direct(
    event_id: str,
    px_context: str = "",
    tabs: Optional[List[str]] = None,
) -> Tuple[Set[str], Dict[str, str], Dict[str, str]]:
    """
    Call the FanDuel event-page API directly for each tab and union the results.

    Returns: (market_ids, market_name_map, market_type_map)
    Each tab's attachments.markets contains only that tab's loaded markets,
    so iterating all tabs maximises the set of discovered market IDs.
    """
    tabs = tabs or EVENT_PAGE_TABS
    market_ids: Set[str] = set()
    market_name_map: Dict[str, str] = {}
    market_type_map: Dict[str, str] = {}

    headers = {k: v for k, v in FD_API_HEADERS.items() if k != "Content-Type"}
    headers["Accept"] = "application/json"
    if px_context:
        headers["x-px-context"] = px_context

    for tab in tabs:
        url = FD_EVENT_PAGE_URL.format(event_id=event_id, tab=tab)
        try:
            resp = requests.get(url, headers=headers, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            if _is_nav_response(data):
                ids = _extract_market_ids_from_nav(data)
                names = _extract_market_names_from_nav(data)
                types = _extract_market_type_raw_from_nav(data)
                market_ids.update(ids)
                market_name_map.update(names)
                market_type_map.update(types)
                logger.info(
                    "  event %s tab=%-12s → %d market IDs", event_id, tab, len(ids)
                )
            else:
                logger.debug("  event %s tab=%s: not a nav response", event_id, tab)
        except Exception as exc:
            logger.warning("  event %s tab=%s failed: %s", event_id, tab, exc)
        time.sleep(0.3)

    return market_ids, market_name_map, market_type_map


# ---------------------------------------------------------------------------
# Discover mode
# ---------------------------------------------------------------------------

def discover(league: str) -> None:
    """Load the competition page with a very broad capture and log all XHRs."""
    cfg = LEAGUE_CONFIG[league.upper()]
    page_url = cfg["url"]
    logger.info("DISCOVERY MODE [%s] → %s", league, page_url)

    result = _call_proxy({
        "url": page_url,
        "prime_url": FD_BASE_URL,
        "capture_patterns": ["fanduel.com"],
        "wait_ms": 30000,
        "timeout_ms": 120000,
    })

    captured = result.get("captured_requests", [])
    logger.info("page_status=%s  total_captured=%d", result.get("status"), len(captured))

    print(f"\n=== DISCOVERED XHRs [{league}] ===")
    for i, cap in enumerate(captured):
        url = cap.get("url", "<unknown>")
        body = cap.get("body")
        body_type = type(body).__name__
        top_keys = list(body.keys())[:6] if isinstance(body, dict) else "n/a"
        req_headers = cap.get("request_headers") or cap.get("requestHeaders") or {}
        has_px = "x-px-context" in {k.lower() for k in req_headers.keys()}
        print(f"[{i:02d}] {url[:120]}")
        print(f"      body_type={body_type}  top_keys={top_keys}  has_px={has_px}")
    print("=== END ===\n")


# ---------------------------------------------------------------------------
# Scrape
# ---------------------------------------------------------------------------

def scrape(league: str, dry_run: bool = False) -> List[Dict[str, Any]]:
    league = league.upper()
    if league not in LEAGUE_CONFIG:
        raise ValueError(f"Unknown league '{league}'. Choose from: {list(LEAGUE_CONFIG)}")

    scraped_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    (
        market_ids,
        event_map,
        market_name_map,
        market_type_map,
        market_to_event,
        px_context,
    ) = _scrape_competition_page(league)

    if not market_ids:
        logger.error("No marketIds found. Run --discover to debug XHR capture.")
        return []

    logger.info(
        "Phase 1 complete: %d marketIds (moneylines), %d events, px=%s",
        len(market_ids), len(event_map), "YES" if px_context else "NO",
    )

    # Phase 2: fetch full market data for near-term events (next 8 days)
    near_term = _identify_near_term_events(event_map)
    logger.info(
        "Phase 2: %d near-term events (next %d days) → fetching all tabs",
        len(near_term), NEAR_TERM_DAYS,
    )
    for event_id in near_term:
        ev_info = event_map.get(event_id, {})
        logger.info(
            "  Fetching %s (%s)",
            ev_info.get("name", event_id), ev_info.get("event_start", "?"),
        )
        ev_ids, ev_names, ev_types = _fetch_event_page_direct(event_id, px_context)
        market_ids.update(ev_ids)
        market_name_map.update(ev_names)
        market_type_map.update(ev_types)
        # Register any new market IDs back to this event
        for mid in ev_ids:
            if mid not in market_to_event:
                market_to_event[mid] = event_id

    logger.info(
        "Phase 2 complete: %d total marketIds across %d events",
        len(market_ids), len(event_map),
    )

    rows = _fetch_market_prices(
        sorted(market_ids),
        scraped_at=scraped_at,
        league=league,
        event_map=event_map,
        market_name_map=market_name_map,
        market_type_map=market_type_map,
        market_to_event=market_to_event,
        px_context=px_context,
    )

    by_type: Dict[str, int] = {}
    for row in rows:
        mt = row.get("market_type", "other")
        by_type[mt] = by_type.get(mt, 0) + 1

    logger.info("FanDuel %s: %d rows, %d market types", league, len(rows), len(by_type))
    for mt, count in sorted(by_type.items(), key=lambda x: -x[1]):
        logger.info("  %-25s %d", mt, count)

    if not rows:
        logger.warning("0 rows parsed.")
        return []

    if dry_run:
        seen: Set[str] = set()
        for row in rows:
            mt = row.get("market_type", "")
            if mt not in seen:
                seen.add(mt)
                print(json.dumps(row, default=str))
        return rows

    artifact = ARTIFACT_PATTERN.format(league=league.lower())
    Path(artifact).parent.mkdir(parents=True, exist_ok=True)
    with open(artifact, "w") as fh:
        for row in rows:
            fh.write(json.dumps(row, default=str) + "\n")
    logger.info("Wrote %d rows to %s", len(rows), artifact)
    return rows


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------

def load(league: str) -> None:
    from google.cloud import bigquery
    from google.cloud.bigquery import LoadJobConfig, SourceFormat
    import io

    league = league.upper()
    artifact = ARTIFACT_PATTERN.format(league=league.lower())
    if not Path(artifact).exists():
        logger.warning("No artifact at %s", artifact)
        return

    rows = []
    with open(artifact) as fh:
        for line in fh:
            line = line.strip()
            if line:
                rows.append(json.loads(line))

    if not rows:
        logger.info("Artifact empty — nothing to load")
        return

    project = os.environ.get("GCP_PROJECT", "graphite-flare-477419-h7")
    client = bigquery.Client(project=project)
    table_id = f"{project}.{DATASET}.{TABLE}"

    ndjson_bytes = "\n".join(json.dumps(r, default=str) for r in rows).encode()
    job_config = LoadJobConfig(
        source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition="WRITE_APPEND",
        autodetect=False,
    )
    job = client.load_table_from_file(io.BytesIO(ndjson_bytes), table_id, job_config=job_config)
    job.result()
    if job.errors:
        logger.error("BigQuery load errors: %s", job.errors)
        sys.exit(1)
    logger.info("Inserted %d rows into %s", len(rows), table_id)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="FanDuel Soccer market scraper v3")
    parser.add_argument("--league", default="MLS", help="MLS or EPL")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--scrape-only", action="store_true")
    group.add_argument("--load-only", action="store_true")
    group.add_argument("--discover", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if args.load_only:
        load(args.league)
    elif args.discover:
        discover(args.league)
    else:
        rows = scrape(args.league, dry_run=args.dry_run)
        if not args.scrape_only and not args.dry_run and rows:
            load(args.league)


if __name__ == "__main__":
    main()

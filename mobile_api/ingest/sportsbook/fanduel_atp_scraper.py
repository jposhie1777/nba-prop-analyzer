"""
FanDuel ATP Tennis Market Scraper — v2 (soccer-style architecture)

Architecture (mirrors fanduel_soccer_scraper.py):
  1. Loads /tennis sport page via Camoufox, captures content-managed-page XHRs
  2. Extracts all marketIds from nav response attachments
  3. Extracts event metadata (player_home, player_away, start) from attachments.events
  4. Filters markets by ATP competition IDs (excludes WTA, ITF, Challenger, UTR)
  5. Parses odds directly from nav market runners (same structure as getMarketPrices)
  6. Falls back to batched getMarketPrices POST if nav runners lack odds
  7. Writes to BigQuery table: sportsbook.raw_fanduel_atp_markets

ATP-only: events are included only when their competition name starts with
"ATP" (e.g. "ATP Houston TX 2026", "ATP Marrakech 2026").

Usage:
  python -m mobile_api.ingest.sportsbook.fanduel_atp_scraper --scrape-only
  python -m mobile_api.ingest.sportsbook.fanduel_atp_scraper --load-only
  python -m mobile_api.ingest.sportsbook.fanduel_atp_scraper --dry-run
  python -m mobile_api.ingest.sportsbook.fanduel_atp_scraper --discover

Environment variables:
  CAMOUFOX_SERVICE_URL             - Cloud Run proxy URL
  CAMOUFOX_TOKEN                   - GCP identity token for the proxy
  GCP_PROJECT                      - GCP project ID
  GOOGLE_APPLICATION_CREDENTIALS   - path to service account JSON (load phase)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
from urllib.parse import urlencode

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DATASET = "sportsbook"
TABLE = "raw_fanduel_atp_markets"
ARTIFACT_PATH = "/tmp/fanduel_atp_rows.ndjson"

FD_TENNIS_URL = "https://sportsbook.fanduel.com/tennis"

FD_MARKET_PRICES_URL = (
    "https://smp.ia.sportsbook.fanduel.com"
    "/api/sports/fixedodds/readonly/v1/getMarketPrices?priceHistory=1"
)

# Event-page API returns ALL markets for a single event (deeper markets)
FD_EVENT_PAGE_URL = (
    "https://api.sportsbook.fanduel.com/sbapi/event-page"
    "?_ak=FhMFpcPWXMeyZxOx"
    "&eventId={event_id}"
    "&useQuickBets=true"
)

# Polite delay between event API calls
EVENT_PAGE_DELAY_S = 0.4

FD_API_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Origin": "https://sportsbook.fanduel.com",
    "Referer": "https://sportsbook.fanduel.com/",
    "x-sportsbook-region": "IA",
    "X-Application": "FhMFpcPWXMeyZxOx",
}

MARKET_PRICES_BATCH_SIZE = 50

CAPTURE_PATTERNS = ["content-managed-page", "getMarketPrices"]

ATP_PREFIX = "ATP"

# ---------------------------------------------------------------------------
# Camoufox proxy
# ---------------------------------------------------------------------------

from mobile_api.ingest.sportsbook.camoufox_client import (
    call_proxy as _call_proxy,
    get_camoufox_url as _get_camoufox_url,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _american_from_decimal_int(decimal: float) -> Optional[int]:
    if decimal <= 1.0:
        return None
    if decimal >= 2.0:
        return int(round((decimal - 1) * 100))
    return int(round(-100 / (decimal - 1)))


def _build_deep_link(market_id: str, selection_id: str) -> str:
    params = urlencode([("marketId[]", market_id), ("selectionId[]", selection_id)])
    return f"fanduelsportsbook://launch?deepLink=addToBetslip%3F{params}"


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


# ---------------------------------------------------------------------------
# Navigation response parsing
# ---------------------------------------------------------------------------

def _extract_market_ids_from_nav(data: Dict[str, Any]) -> Set[str]:
    """Extract all marketIds from nav attachments.
    Tennis uses marketId (like soccer), not externalMarketId like golf."""
    market_ids: Set[str] = set()
    for market in data.get("attachments", {}).get("markets", {}).values():
        if not isinstance(market, dict):
            continue
        mid = market.get("marketId") or market.get("externalMarketId")
        if mid:
            market_ids.add(str(mid))
        for assoc in market.get("associatedMarkets", []):
            if isinstance(assoc, dict):
                aid = assoc.get("marketId") or assoc.get("externalMarketId")
                if aid:
                    market_ids.add(str(aid))
    return market_ids


def _extract_market_names_from_nav(data: Dict[str, Any]) -> Dict[str, str]:
    """Build marketId → marketName from navigation attachments."""
    market_names: Dict[str, str] = {}
    for market in data.get("attachments", {}).get("markets", {}).values():
        if not isinstance(market, dict):
            continue
        mid = str(market.get("marketId") or market.get("externalMarketId") or "")
        name = market.get("marketName") or market.get("name") or ""
        if mid and name:
            market_names[mid] = name
        for assoc in market.get("associatedMarkets", []):
            if not isinstance(assoc, dict):
                continue
            aid = str(assoc.get("marketId") or assoc.get("externalMarketId") or "")
            aname = assoc.get("marketName") or assoc.get("name") or name
            if aid and aname:
                market_names[aid] = aname
    return market_names


def _extract_market_event_map(data: Dict[str, Any]) -> Dict[str, str]:
    """Build marketId → eventId from nav attachments.markets."""
    mapping: Dict[str, str] = {}
    for market in data.get("attachments", {}).get("markets", {}).values():
        if not isinstance(market, dict):
            continue
        event_id = str(market.get("eventId") or "")
        if not event_id:
            continue
        mid = str(market.get("marketId") or market.get("externalMarketId") or "")
        if mid:
            mapping[mid] = event_id
        for assoc in market.get("associatedMarkets", []):
            if isinstance(assoc, dict):
                aid = str(assoc.get("marketId") or assoc.get("externalMarketId") or "")
                if aid:
                    mapping[aid] = event_id
    return mapping


def _extract_events(data: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    Build eventId → {player_home, player_away, event_start, event_id,
    tournament_name, competition_id} from nav attachments.events.
    """
    competitions = data.get("attachments", {}).get("competitions", {})
    events: Dict[str, Dict[str, Any]] = {}
    for ev_id, ev in data.get("attachments", {}).get("events", {}).items():
        if not isinstance(ev, dict):
            continue
        name = ev.get("name") or ""
        player_home = ""
        player_away = ""
        # FanDuel names tennis events like "Player A v Player B"
        for sep in (" v ", " vs "):
            if sep in name:
                parts = name.split(sep, 1)
                player_home = parts[0].strip()
                player_away = parts[1].strip()
                break
        if not player_home:
            player_home = name

        start_raw = ev.get("openDate") or ev.get("startTime") or ""
        try:
            event_start = (
                datetime.fromisoformat(start_raw.replace("Z", "+00:00")).strftime(
                    "%Y-%m-%dT%H:%M:%S"
                )
                if start_raw
                else None
            )
        except Exception:
            event_start = None

        comp_id = str(ev.get("competitionId", ""))
        comp = competitions.get(comp_id, {})
        tournament_name = comp.get("name", "")

        events[str(ev_id)] = {
            "event_id": str(ev_id),
            "player_home": player_home or None,
            "player_away": player_away or None,
            "event_start": event_start,
            "tournament_name": tournament_name,
            "competition_id": comp_id,
        }
    return events


def _extract_runner_names_from_nav(data: Dict[str, Any]) -> Dict[str, str]:
    """Build selectionId → runnerName from nav attachments."""
    runner_map: Dict[str, str] = {}
    for market in data.get("attachments", {}).get("markets", {}).values():
        if not isinstance(market, dict):
            continue
        for runner in market.get("runners", []):
            if not isinstance(runner, dict):
                continue
            sel_id = str(runner.get("selectionId", ""))
            name = runner.get("runnerName", "")
            if sel_id and name:
                runner_map[sel_id] = name
    return runner_map


# ---------------------------------------------------------------------------
# Parse market data into rows
# ---------------------------------------------------------------------------

def _parse_market_prices_response(
    body: List[Any],
    scraped_at: str,
    event_map: Dict[str, Dict[str, Any]],
    market_event_map: Dict[str, str],
    market_name_map: Optional[Dict[str, str]] = None,
    runner_name_map: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not isinstance(body, list):
        return rows

    market_name_map = market_name_map or {}
    runner_name_map = runner_name_map or {}

    for market in body:
        if not isinstance(market, dict):
            continue

        market_id = str(market.get("marketId", ""))
        market_name = (
            market_name_map.get(market_id)
            or market.get("marketName")
            or market.get("name")
            or ""
        )
        market_type = market.get("marketType", "") or market_name
        market_status = market.get("marketStatus", "")
        sgm_market = bool(market.get("sgmMarket", False))
        inplay = bool(market.get("inPlay", market.get("inplay", False)))
        sort_priority = market.get("sortPriority")
        runners = market.get("runnerDetails", [])

        if not market_id or not runners:
            continue

        # Look up event metadata via market → event mapping
        event_id = market_event_map.get(market_id, "")
        event_meta = event_map.get(event_id, {})
        player_home = event_meta.get("player_home")
        player_away = event_meta.get("player_away")
        event_start = event_meta.get("event_start")
        tournament_name = event_meta.get("tournament_name", "")
        competition_id = event_meta.get("competition_id", "")

        for runner in runners:
            if not isinstance(runner, dict):
                continue

            selection_id = str(runner.get("selectionId", ""))
            selection_name = (
                runner.get("runnerName")
                or runner_name_map.get(selection_id, "")
            )
            runner_status = runner.get("runnerStatus", "")
            handicap = runner.get("handicap")
            sort_order = runner.get("sortPriority")

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
                    odds_am = int(am_val)
            except Exception:
                pass
            if odds_dec is not None and odds_am is None:
                odds_am = _american_from_decimal_int(odds_dec)

            # Previous odds (line movement)
            prev_list = runner.get("previousWinRunnerOdds", [])
            prev_am = None
            if prev_list and isinstance(prev_list[0], dict):
                prev_am_obj = prev_list[0].get("americanDisplayOdds", {})
                try:
                    prev_am = int(prev_am_obj.get("americanOdds"))
                except (TypeError, ValueError):
                    pass

            deep_link = (
                _build_deep_link(market_id, selection_id)
                if market_id and selection_id else None
            )

            rows.append(
                {
                    "scraped_at": scraped_at,
                    "league": "ATP",
                    "tournament_name": tournament_name or None,
                    "competition_id": competition_id or None,
                    "event_id": event_id or None,
                    "event_name": (
                        f"{player_home} v {player_away}"
                        if player_home and player_away
                        else player_home
                    ),
                    "player_home": player_home,
                    "player_away": player_away,
                    "event_start": event_start,
                    "is_inplay": inplay,
                    "market_id": market_id or None,
                    "market_type": market_type or None,
                    "market_name": market_name or None,
                    "market_sort_priority": sort_priority,
                    "sgm_market": sgm_market,
                    "selection_id": selection_id or None,
                    "selection_name": selection_name or None,
                    "runner_status": runner_status or None,
                    "runner_sort_priority": sort_order,
                    "handicap": handicap_f,
                    "odds_decimal": odds_dec,
                    "odds_american": odds_am,
                    "odds_american_prev": prev_am,
                    "deep_link": deep_link,
                }
            )

    return rows


# ---------------------------------------------------------------------------
# getMarketPrices batched POST (fallback if nav runners lack odds)
# ---------------------------------------------------------------------------

def _fetch_market_prices(
    market_ids: List[str],
    scraped_at: str,
    event_map: Dict[str, Dict[str, Any]],
    market_event_map: Dict[str, str],
    market_name_map: Optional[Dict[str, str]] = None,
    runner_name_map: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    all_rows: List[Dict[str, Any]] = []
    batches = [
        market_ids[i : i + MARKET_PRICES_BATCH_SIZE]
        for i in range(0, len(market_ids), MARKET_PRICES_BATCH_SIZE)
    ]

    headers = {**FD_API_HEADERS}

    for i, batch in enumerate(batches):
        logger.info(
            "getMarketPrices batch %d/%d (%d ids)", i + 1, len(batches), len(batch)
        )
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
                scraped_at,
                event_map,
                market_event_map,
                market_name_map,
                runner_name_map,
            )
            logger.info("  → %d rows", len(rows))
            all_rows.extend(rows)
        except Exception as exc:
            logger.warning("Batch %d failed: %s", i + 1, exc)
        if i < len(batches) - 1:
            time.sleep(0.5)

    return all_rows


# ---------------------------------------------------------------------------
# Scrape phase
# ---------------------------------------------------------------------------

def scrape(dry_run: bool = False) -> List[Dict[str, Any]]:
    scraped_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Phase 1: Load /tennis sport page via Camoufox
    # Prime with the tennis URL itself (like PGA scraper) to warm PerimeterX cookies
    logger.info("Loading FanDuel Tennis via Camoufox: %s", FD_TENNIS_URL)
    result = _call_proxy(
        {
            "url": FD_TENNIS_URL,
            "prime_url": FD_TENNIS_URL,
            "capture_patterns": CAPTURE_PATTERNS,
            "wait_ms": 45000,
            "timeout_ms": 120000,
        }
    )

    page_status = result.get("status", 0)
    captured = result.get("captured_requests", [])
    logger.info(
        "FanDuel Tennis: page_status=%s, captured=%d requests",
        page_status,
        len(captured),
    )

    all_market_ids: Set[str] = set()
    market_name_map: Dict[str, str] = {}
    market_event_map: Dict[str, str] = {}
    all_event_map: Dict[str, Dict[str, Any]] = {}
    runner_name_map: Dict[str, str] = {}
    nav_market_list: List[Dict[str, Any]] = []
    # Track competitionId per market and event for ATP filtering
    market_competition: Dict[str, str] = {}
    event_competition: Dict[str, str] = {}
    # Track which competition names are ATP
    atp_comp_ids: Set[str] = set()
    # Capture px-context token for direct API calls
    px_context = ""

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
        logger.info(
            "  [%02d] %s  body=%s  preview=%r",
            i,
            cap_url[:120],
            body_type,
            preview,
        )

        # Capture px-context token from request headers
        if not px_context and isinstance(req_headers, dict):
            for k, v in req_headers.items():
                if k.lower() == "x-px-context":
                    px_context = str(v)
                    logger.info("  → x-px-context captured (%d chars)", len(px_context))
                    break

        # content-managed-page → nav JSON with events + markets
        if "content-managed-page" in cap_url:
            nav_data = _try_parse_json(cap_body)
            if not nav_data or (
                "layout" not in nav_data and "attachments" not in nav_data
            ):
                continue

            nav_ids = _extract_market_ids_from_nav(nav_data)
            nav_names = _extract_market_names_from_nav(nav_data)
            nav_mkt_events = _extract_market_event_map(nav_data)
            nav_events = _extract_events(nav_data)
            nav_runners = _extract_runner_names_from_nav(nav_data)

            all_market_ids.update(nav_ids)
            market_name_map.update(nav_names)
            market_event_map.update(nav_mkt_events)
            all_event_map.update(nav_events)
            runner_name_map.update(nav_runners)

            # Discover ATP competition IDs dynamically from competition names
            competitions = nav_data.get("attachments", {}).get("competitions", {})
            for comp_id, comp in competitions.items():
                if isinstance(comp, dict) and comp.get("name", "").startswith(
                    ATP_PREFIX
                ):
                    atp_comp_ids.add(str(comp_id))

            logger.info(
                "  → ATP competitions: %s",
                [
                    competitions[c]["name"]
                    for c in atp_comp_ids
                    if c in competitions
                ],
            )

            # Extract competitionId per event and per market for filtering
            for ev_id, ev in (
                nav_data.get("attachments", {}).get("events", {}).items()
            ):
                if isinstance(ev, dict) and ev.get("competitionId"):
                    event_competition[str(ev_id)] = str(ev["competitionId"])
            for mk in (
                nav_data.get("attachments", {}).get("markets", {}).values()
            ):
                if isinstance(mk, dict) and mk.get("competitionId"):
                    mid = str(mk.get("marketId") or "")
                    if mid:
                        market_competition[mid] = str(mk["competitionId"])

            logger.info(
                "  → content-managed-page: %d marketIds, %d events, %d market names, %d runners",
                len(nav_ids),
                len(nav_events),
                len(nav_names),
                len(nav_runners),
            )

        # getMarketPrices — capture for deeper markets
        if "getMarketPrices" in cap_url:
            if isinstance(cap_body, list):
                logger.info(
                    "  → getMarketPrices response: %d markets", len(cap_body)
                )
                # Extract eventId → marketId mapping and runner names from
                # getMarketPrices responses (these contain deeper markets not
                # in the nav data: set betting, handicaps, totals, etc.)
                for mkt in cap_body:
                    if not isinstance(mkt, dict):
                        continue
                    mid = str(mkt.get("marketId", ""))
                    eid = str(mkt.get("eventId", ""))
                    mname = mkt.get("marketName") or mkt.get("name") or ""
                    if mid and eid:
                        market_event_map[mid] = eid
                    if mid and mname:
                        market_name_map[mid] = mname
                    for runner in mkt.get("runnerDetails", []):
                        if not isinstance(runner, dict):
                            continue
                        sel_id = str(runner.get("selectionId", ""))
                        rname = runner.get("runnerName", "")
                        if sel_id and rname:
                            runner_name_map[sel_id] = rname

    if not atp_comp_ids:
        logger.warning(
            "No ATP competitions found in nav data. "
            "All competitions may be non-ATP or page returned no data."
        )
        return []

    # Filter markets by ATP competition IDs
    # Markets that have competitionId directly
    league_market_ids: Set[str] = {
        mid for mid, cid in market_competition.items() if cid in atp_comp_ids
    }
    # Also include markets linked to ATP events as fallback
    league_event_ids: Set[str] = {
        eid for eid, cid in event_competition.items() if cid in atp_comp_ids
    }
    for mid, eid in market_event_map.items():
        if eid in league_event_ids:
            league_market_ids.add(mid)

    # Build nav_market_list: raw nav market dicts for ATP only.
    # Rename 'runners' → 'runnerDetails' for parser compatibility.
    for cap in captured:
        cap_url = cap.get("url", "")
        cap_body = cap.get("body")
        if "content-managed-page" not in cap_url:
            continue
        nav_data = _try_parse_json(cap_body)
        if not nav_data:
            continue
        for mk in nav_data.get("attachments", {}).get("markets", {}).values():
            if not isinstance(mk, dict):
                continue
            mid = str(mk.get("marketId", ""))
            if mid in league_market_ids:
                mk_copy = dict(mk)
                if "runners" in mk_copy and "runnerDetails" not in mk_copy:
                    mk_copy["runnerDetails"] = mk_copy.pop("runners")
                nav_market_list.append(mk_copy)

    logger.info(
        "FanDuel ATP: %d/%d nav markets match ATP filter, %d ATP events, px=%s",
        len(league_market_ids),
        len(all_market_ids),
        len(league_event_ids),
        "YES" if px_context else "NO",
    )

    if not league_event_ids:
        logger.warning(
            "FanDuel ATP: 0 ATP events after competition filter (atp_comp_ids=%s). "
            "Possible causes: no fixtures, or no ATP events today.",
            atp_comp_ids,
        )
        return []

    # Phase 2a: Parse odds from nav market data (top-level markets like Moneyline).
    rows = _parse_market_prices_response(
        nav_market_list,
        scraped_at=scraped_at,
        event_map=all_event_map,
        market_event_map=market_event_map,
        market_name_map=market_name_map,
        runner_name_map=runner_name_map,
    )
    logger.info("  → %d rows from nav markets (Moneyline)", len(rows))

    # Phase 2b: Fetch deeper markets per ATP event via event-page API.
    # The sport page only has top-level markets (Moneyline). Deeper markets
    # (set betting, game handicaps, totals, correct scores, etc.) require
    # loading each event's page.
    deeper_market_ids: Set[str] = set()
    seen_nav_ids = {str(mk.get("marketId", "")) for mk in nav_market_list}

    # Build headers with px-context for direct API calls
    event_headers = {**FD_API_HEADERS}
    if px_context:
        event_headers["x-px-context"] = px_context

    # Skip doubles matches (no deep markets typically) to save API calls
    singles_events = [
        eid for eid in league_event_ids
        if eid in all_event_map and "/" not in (all_event_map[eid].get("player_home") or "")
    ]
    logger.info(
        "Phase 2b: Fetching deeper markets for %d singles events (%d total ATP events)",
        len(singles_events),
        len(league_event_ids),
    )

    for i, event_id in enumerate(sorted(singles_events), 1):
        event_meta = all_event_map.get(event_id, {})
        event_label = (
            f"{event_meta.get('player_home', '?')} v {event_meta.get('player_away', '?')}"
        )
        api_url = FD_EVENT_PAGE_URL.format(event_id=event_id)

        try:
            resp = requests.get(api_url, headers=event_headers, timeout=30)
            resp.raise_for_status()
            body = resp.json()

            attachments = body.get("attachments", {}) if isinstance(body, dict) else {}
            event_markets = attachments.get("markets", {})

            if not event_markets:
                logger.info(
                    "  [%d/%d] %s → 0 markets", i, len(singles_events), event_label
                )
            else:
                new_count = 0
                for mid, mk in event_markets.items():
                    if not isinstance(mk, dict):
                        continue
                    mid_str = str(mk.get("marketId", mid))
                    mname = mk.get("marketName") or mk.get("name") or ""
                    if mid_str and mname:
                        market_name_map[mid_str] = mname
                    if mid_str:
                        market_event_map[mid_str] = event_id
                    # Skip markets we already have from nav
                    if mid_str in seen_nav_ids:
                        continue
                    seen_nav_ids.add(mid_str)
                    deeper_market_ids.add(mid_str)
                    new_count += 1
                    # Extract runner names
                    for runner in mk.get("runners", []):
                        if isinstance(runner, dict):
                            sel_id = str(runner.get("selectionId", ""))
                            rname = runner.get("runnerName", "")
                            if sel_id and rname:
                                runner_name_map[sel_id] = rname
                    # Add market to nav_market_list for direct parsing
                    mk_copy = dict(mk)
                    if "runners" in mk_copy and "runnerDetails" not in mk_copy:
                        mk_copy["runnerDetails"] = mk_copy.pop("runners")
                    nav_market_list.append(mk_copy)

                logger.info(
                    "  [%d/%d] %s → %d markets (%d new deeper)",
                    i, len(singles_events), event_label,
                    len(event_markets), new_count,
                )
        except Exception as exc:
            logger.warning(
                "  [%d/%d] %s → event-page failed: %s",
                i, len(singles_events), event_label, exc,
            )

        if i < len(singles_events):
            time.sleep(EVENT_PAGE_DELAY_S)

    # Phase 2c: Parse deeper markets from event pages.
    if deeper_market_ids:
        # Re-parse all nav_market_list (now includes deeper markets) but only
        # keep the new deeper ones to avoid duplicating Phase 2a rows.
        deeper_only = [
            mk for mk in nav_market_list
            if str(mk.get("marketId", "")) in deeper_market_ids
        ]
        deeper_rows = _parse_market_prices_response(
            deeper_only,
            scraped_at=scraped_at,
            event_map=all_event_map,
            market_event_map=market_event_map,
            market_name_map=market_name_map,
            runner_name_map=runner_name_map,
        )
        logger.info(
            "  → %d rows from %d deeper markets across %d events",
            len(deeper_rows), len(deeper_market_ids), len(singles_events),
        )
        rows.extend(deeper_rows)

    # Phase 2d: If event-page API didn't work (no px token, blocked, etc.),
    # fall back to fetching getMarketPrices directly for nav market IDs.
    if not rows and league_market_ids:
        logger.info(
            "No rows from captured data — falling back to direct getMarketPrices for %d markets",
            len(league_market_ids),
        )
        rows = _fetch_market_prices(
            list(league_market_ids),
            scraped_at=scraped_at,
            event_map=all_event_map,
            market_event_map=market_event_map,
            market_name_map=market_name_map,
            runner_name_map=runner_name_map,
        )

    logger.info("FanDuel ATP: %d total rows", len(rows))

    if not rows:
        logger.warning(
            "FanDuel ATP: 0 rows parsed from %d marketIds.", len(league_market_ids)
        )

    if dry_run:
        for row in rows[:10]:
            print(json.dumps(row, default=str))
        return rows

    Path(ARTIFACT_PATH).parent.mkdir(parents=True, exist_ok=True)
    with open(ARTIFACT_PATH, "w") as fh:
        for row in rows:
            fh.write(json.dumps(row, default=str) + "\n")
    logger.info("Wrote %d rows to %s", len(rows), ARTIFACT_PATH)
    return rows


# ---------------------------------------------------------------------------
# Load phase
# ---------------------------------------------------------------------------

def load() -> None:
    from google.cloud import bigquery
    from google.cloud.bigquery import LoadJobConfig, SourceFormat
    import io

    if not Path(ARTIFACT_PATH).exists():
        logger.warning("No artifact at %s — nothing to load", ARTIFACT_PATH)
        return

    rows = []
    with open(ARTIFACT_PATH) as fh:
        for line in fh:
            line = line.strip()
            if line:
                rows.append(json.loads(line))

    if not rows:
        logger.info("Artifact is empty — nothing to load")
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
    job = client.load_table_from_file(
        io.BytesIO(ndjson_bytes),
        table_id,
        job_config=job_config,
    )
    job.result()
    if job.errors:
        logger.error("BigQuery load errors: %s", job.errors)
        sys.exit(1)
    logger.info("Inserted %d rows into %s", len(rows), table_id)


# ---------------------------------------------------------------------------
# Discover mode
# ---------------------------------------------------------------------------

def discover() -> None:
    """Load the tennis page with broad capture patterns and log all XHRs."""
    logger.info("DISCOVERY MODE → %s", FD_TENNIS_URL)
    result = _call_proxy(
        {
            "url": FD_TENNIS_URL,
            "prime_url": FD_TENNIS_URL,
            "capture_patterns": ["fanduel.com"],
            "wait_ms": 45000,
            "timeout_ms": 120000,
        }
    )

    captured = result.get("captured_requests", [])
    logger.info(
        "page_status=%s  total_captured=%d", result.get("status"), len(captured)
    )

    print("\n=== DISCOVERED XHRs for ATP Tennis ===")
    for i, cap in enumerate(captured):
        url = cap.get("url", "<unknown>")
        body = cap.get("body")
        body_type = type(body).__name__
        top_keys = list(body.keys())[:6] if isinstance(body, dict) else "n/a"
        body_len = len(body) if isinstance(body, (str, list)) else "n/a"
        print(f"[{i:02d}] {url[:140]}")
        print(f"      body_type={body_type}  top_keys={top_keys}  len={body_len}")

        # Show competitions if this is nav data
        if isinstance(body, dict) and "attachments" in body:
            comps = body.get("attachments", {}).get("competitions", {})
            events = body.get("attachments", {}).get("events", {})
            markets = body.get("attachments", {}).get("markets", {})
            print(f"      competitions={len(comps)}  events={len(events)}  markets={len(markets)}")
            for cid, comp in comps.items():
                if isinstance(comp, dict):
                    print(f"        comp {cid}: {comp.get('name', '?')}")
    print("=== END ===\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="FanDuel ATP tennis market scraper v2")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--scrape-only", action="store_true")
    group.add_argument("--load-only", action="store_true")
    group.add_argument("--discover", action="store_true")
    parser.add_argument("--dry-run", action="store_true", help="Print rows, skip BQ write")
    args = parser.parse_args()

    if args.load_only:
        load()
    elif args.discover:
        discover()
    else:
        scrape(dry_run=args.dry_run)
        if not args.scrape_only and not args.dry_run:
            load()


if __name__ == "__main__":
    main()

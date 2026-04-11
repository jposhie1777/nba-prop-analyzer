"""
FanDuel PGA Tour Market Scraper — v6 (multi-tournament)

Architecture:
  1. Discovers ALL available golf events from FanDuel application-context API
  2. For each tournament, loads its page via Camoufox and captures XHRs
  3. From captured requests:
       - Navigation response body  → all externalMarketIds + player names
       - getMarketPrices POST bodies → additional marketIds
       - Any request headers       → x-px-context token
  4. POSTs all discovered marketIds to getMarketPrices directly (with px token)
  5. Classifies markets, builds deep links, writes to BigQuery
  6. Each row is tagged with tournament_name + tournament_slug for multi-event support

Usage:
  python -m mobile_api.ingest.sportsbook.fanduel_pga_scraper --dry-run
  python -m mobile_api.ingest.sportsbook.fanduel_pga_scraper --scrape-only
  python -m mobile_api.ingest.sportsbook.fanduel_pga_scraper --load-only
  python -m mobile_api.ingest.sportsbook.fanduel_pga_scraper --discover
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
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
TABLE = "raw_fanduel_pga_markets"
ARTIFACT_PATH = "/tmp/fanduel_pga_rows.ndjson"

FD_BASE_URL = "https://sportsbook.fanduel.com"
FD_GOLF_URL = f"{FD_BASE_URL}/golf"
FD_PRIME_URL = FD_BASE_URL  # homepage warms up PerimeterX cookies

FD_APP_CONTEXT_URL = (
    "https://api.sportsbook.fanduel.com/sbapi/application-context"
    "?dataEntries=POPULAR_BETTING,QUICK_LINKS,AZ_BETTING,EVENT_TYPES,TEASER_COMPS"
    "&_ak=FhMFpcPWXMeyZxOx"
)
FD_GOLF_EVENT_TYPE_ID = "3"

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
    "x-sportsbook-region": "IA",
    "X-Application": "FhMFpcPWXMeyZxOx",
}

MARKET_PRICES_BATCH_SIZE = 50

# Exact substrings matching the URLs seen in --discover output:
#   [50] api.sportsbook.fanduel.com/sbapi/content-managed-page  → layout + attachments (navigation)
#   [54/55/58] smp.ia.sportsbook.fanduel.com/.../getMarketPrices  → market odds (list)
CAPTURE_PATTERNS = [
    "content-managed-page",
    "getMarketPrices",
]

ROUND_SCORE_SELECTION_IDS = {
    "23730687", "23730688",
    "16274521", "16274522",
    "68613232", "23746580",
}

HOLE_SCORE_SELECTION_IDS = {
    "61579324", "61579325", "13543690",
}


# ---------------------------------------------------------------------------
# Camoufox proxy helpers
# ---------------------------------------------------------------------------

from mobile_api.ingest.sportsbook.camoufox_client import (
    call_proxy as _call_proxy,
    get_camoufox_url as _get_camoufox_url,
)


# ---------------------------------------------------------------------------
# Tournament URL discovery
# ---------------------------------------------------------------------------

def _slug_from_name(name: str) -> str:
    """Generate a FanDuel-style URL slug from a tournament name."""
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


def _scrape_golf_landing() -> Tuple[
    List[Dict[str, str]], Dict[str, Dict[str, Any]], Set[str], str
]:
    """
    Load fanduel.com/golf ONCE via Camoufox and extract:
      - All tournament events (with names, IDs, start dates)
      - Per-event market data (market_ids, player_map, market_name_map) keyed by eventId
      - All market IDs (union across events, for fallback)
      - px_context token for subsequent API calls

    Returns: (tournaments, per_event_data, all_market_ids, px_context)
    """
    tournaments: List[Dict[str, str]] = []
    all_market_ids: Set[str] = set()
    per_event_data: Dict[str, Dict[str, Any]] = {}
    px_context = ""

    logger.info("Loading golf landing page via Camoufox: %s", FD_GOLF_URL)
    result = _call_proxy({
        "url": FD_GOLF_URL,
        "prime_url": FD_GOLF_URL,
        "capture_patterns": CAPTURE_PATTERNS,
        "wait_ms": 30000,
        "timeout_ms": 120000,
    })

    page_status = result.get("status", 0)
    captured = result.get("captured_requests", [])
    logger.info("Landing page: status=%s, captured=%d requests", page_status, len(captured))

    for cap in captured:
        cap_url = cap.get("url", "")
        cap_body = cap.get("body")
        req_headers = cap.get("request_headers") or cap.get("requestHeaders") or {}

        # Capture px token
        if not px_context and isinstance(req_headers, dict):
            for k, v in req_headers.items():
                if k.lower() == "x-px-context":
                    px_context = str(v)
                    logger.info("  → x-px-context captured (%d chars)", len(px_context))
                    break

        # content-managed-page → navigation JSON with events + markets
        if "content-managed-page" in cap_url:
            nav_data = _try_parse_json(cap_body)
            if not nav_data or ("layout" not in nav_data and "attachments" not in nav_data):
                continue

            # Extract all market IDs (flat, for fallback)
            nav_ids = _extract_market_ids_from_nav(nav_data)
            all_market_ids.update(nav_ids)

            # Extract per-event data (market_ids, player_map, market_name_map per tournament)
            event_data = _extract_per_event_data(nav_data)
            for eid, bucket in event_data.items():
                if eid in per_event_data:
                    per_event_data[eid]["market_ids"].update(bucket["market_ids"])
                    per_event_data[eid]["player_map"].update(bucket["player_map"])
                    per_event_data[eid]["market_name_map"].update(bucket["market_name_map"])
                else:
                    per_event_data[eid] = bucket

            # Extract tournament events
            events = nav_data.get("attachments", {}).get("events", {})
            for ev_id, ev in events.items():
                if not isinstance(ev, dict):
                    continue
                name = (ev.get("name") or ev.get("eventName") or "").strip()
                seo = ev.get("seoIdentifier") or ev.get("slug") or ""
                start = ev.get("openDate") or ev.get("startTime") or ""
                if not name:
                    continue
                slug = seo or _slug_from_name(name)
                url = f"{FD_BASE_URL}/golf/{slug}-{ev_id}"
                tournaments.append({
                    "id": str(ev_id),
                    "slug": slug,
                    "name": name,
                    "url": url,
                    "start": start,
                })

            logger.info(
                "  → nav: %d events, %d marketIds total, %d events with per-event data",
                len(events), len(nav_ids), len(event_data),
            )

        # getMarketPrices responses — capture additional market IDs
        if "getMarketPrices" in cap_url:
            if isinstance(cap_body, list):
                ids = [str(m["marketId"]) for m in cap_body if isinstance(m, dict) and m.get("marketId")]
                all_market_ids.update(ids)
            else:
                body_data = _try_parse_json(cap_body)
                if isinstance(body_data, dict) and "marketIds" in body_data:
                    ids = [str(m) for m in body_data["marketIds"] if m]
                    all_market_ids.update(ids)

    # Deduplicate tournaments by name
    seen: Set[str] = set()
    unique: List[Dict[str, str]] = []
    for t in tournaments:
        if t["name"] not in seen:
            seen.add(t["name"])
            unique.append(t)
    tournaments = sorted(unique, key=lambda t: t.get("start", ""))
    for t in tournaments:
        evt_count = len(per_event_data.get(t["id"], {}).get("market_ids", set()))
        logger.info(
            "Discovered tournament: %s → %s (starts %s, %d market IDs)",
            t["name"], t["url"], t["start"], evt_count,
        )

    if not tournaments:
        logger.warning("No tournaments found on landing page")

    logger.info(
        "Landing page result: %d tournaments, %d total marketIds, %d per-event buckets, px=%s",
        len(tournaments), len(all_market_ids), len(per_event_data),
        "YES" if px_context else "NO",
    )
    return tournaments, per_event_data, all_market_ids, px_context


# ---------------------------------------------------------------------------
# Navigation response parsing
# ---------------------------------------------------------------------------

def _extract_market_ids_from_nav(data: Dict[str, Any]) -> Set[str]:
    market_ids: Set[str] = set()
    coupons = data.get("layout", {}).get("coupons", {})
    for coupon in coupons.values():
        if not isinstance(coupon, dict):
            continue
        ext_id = coupon.get("externalMarketId")
        if ext_id:
            market_ids.add(str(ext_id))
        for display_item in coupon.get("display", []):
            if not isinstance(display_item, dict):
                continue
            for row in display_item.get("rows", []):
                if not isinstance(row, dict):
                    continue
                for mid in row.get("marketIds", []):
                    if mid:
                        market_ids.add(str(mid))
    for market in data.get("attachments", {}).get("markets", {}).values():
        if not isinstance(market, dict):
            continue
        for assoc in market.get("associatedMarkets", []):
            if isinstance(assoc, dict) and assoc.get("externalMarketId"):
                market_ids.add(str(assoc["externalMarketId"]))
    return market_ids


def _extract_market_names_from_nav(data: Dict[str, Any]) -> Dict[str, str]:
    """Build externalMarketId → marketName from navigation attachments."""
    market_names: Dict[str, str] = {}
    for market_id, market in data.get("attachments", {}).get("markets", {}).items():
        if not isinstance(market, dict):
            continue
        name = market.get("marketName") or market.get("name") or ""
        ext_id = market.get("externalMarketId") or market_id
        if ext_id and name:
            market_names[str(ext_id)] = name
        # Also index associated markets
        for assoc in market.get("associatedMarkets", []):
            if not isinstance(assoc, dict):
                continue
            aid = assoc.get("externalMarketId")
            aname = assoc.get("marketName") or assoc.get("name") or name
            if aid and aname:
                market_names[str(aid)] = aname
    return market_names


def _extract_per_event_data(data: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    Extract market IDs, player names, and market names grouped by eventId.

    Returns: { eventId: { "market_ids": set, "player_map": dict, "market_name_map": dict } }
    """
    per_event: Dict[str, Dict[str, Any]] = {}

    for market_id, market in data.get("attachments", {}).get("markets", {}).items():
        if not isinstance(market, dict):
            continue

        event_id = str(market.get("eventId") or "")
        if not event_id:
            continue

        if event_id not in per_event:
            per_event[event_id] = {
                "market_ids": set(),
                "player_map": {},
                "market_name_map": {},
            }
        bucket = per_event[event_id]

        ext_id = str(market.get("externalMarketId") or market_id)
        if ext_id:
            bucket["market_ids"].add(ext_id)

        name = market.get("marketName") or market.get("name") or ""
        if ext_id and name:
            bucket["market_name_map"][ext_id] = name

        for assoc in market.get("associatedMarkets", []):
            if not isinstance(assoc, dict):
                continue
            aid = str(assoc.get("externalMarketId") or "")
            if aid:
                bucket["market_ids"].add(aid)
                aname = assoc.get("marketName") or assoc.get("name") or name
                if aname:
                    bucket["market_name_map"][aid] = aname

        for runner in market.get("runners", []):
            if not isinstance(runner, dict):
                continue
            sel_id = str(runner.get("selectionId", ""))
            rname = runner.get("runnerName", "")
            if sel_id and rname:
                bucket["player_map"][sel_id] = rname

    return per_event


def _extract_player_names_from_nav(data: Dict[str, Any]) -> Dict[str, str]:
    player_map: Dict[str, str] = {}
    for market in data.get("attachments", {}).get("markets", {}).values():
        if not isinstance(market, dict):
            continue
        for runner in market.get("runners", []):
            if not isinstance(runner, dict):
                continue
            sel_id = str(runner.get("selectionId", ""))
            name = runner.get("runnerName", "")
            if sel_id and name:
                player_map[sel_id] = name
    return player_map


def _extract_event_name_from_nav(data: Dict[str, Any]) -> str:
    for ev in data.get("attachments", {}).get("events", {}).values():
        if isinstance(ev, dict) and ev.get("name"):
            return ev["name"].strip()
    return ""


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
# Main page load + capture extraction
# ---------------------------------------------------------------------------

def _scrape_tournament_page(
    tournament_url: Optional[str] = None,
) -> Tuple[Set[str], Dict[str, str], str, str]:
    """
    Load the tournament page via Camoufox and extract everything from captures.

    Returns: (market_ids, player_map, event_name, px_context)
    """
    market_ids: Set[str] = set()
    player_map: Dict[str, str] = {}
    market_name_map: Dict[str, str] = {}
    event_name = ""
    px_context = ""

    if not tournament_url:
        tournament_url = FD_GOLF_URL
    logger.info("Loading via Camoufox: %s", tournament_url)

    result = _call_proxy({
        "url": tournament_url,
        "prime_url": tournament_url,
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

        # content-managed-page → this IS the navigation JSON (layout + attachments)
        if "content-managed-page" in cap_url:
            nav_data = _try_parse_json(cap_body)
            if nav_data and ("layout" in nav_data or "attachments" in nav_data):
                nav_ids = _extract_market_ids_from_nav(nav_data)
                nav_players = _extract_player_names_from_nav(nav_data)
                nav_names = _extract_market_names_from_nav(nav_data)
                market_ids.update(nav_ids)
                player_map.update(nav_players)
                market_name_map.update(nav_names)
                if not event_name:
                    event_name = _extract_event_name_from_nav(nav_data)
                logger.info(
                    "  → content-managed-page: %d marketIds, %d players, %d market names",
                    len(nav_ids), len(nav_players), len(nav_names),
                )

        # getMarketPrices — response body is a list of markets (already fetched by page)
        # Extract marketIds from the response so we know what exists
        if "getMarketPrices" in cap_url:
            if isinstance(cap_body, list):
                ids = [str(mkt["marketId"]) for mkt in cap_body if isinstance(mkt, dict) and mkt.get("marketId")]
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
        "Result: %d marketIds, %d players, %d market names, event=%r, px=%s",
        len(market_ids), len(player_map), len(market_name_map), event_name, "YES" if px_context else "NO",
    )
    return market_ids, player_map, market_name_map, event_name, px_context


# ---------------------------------------------------------------------------
# Market classification
# ---------------------------------------------------------------------------

def _classify_market(runners: List[Dict], turn_in_play: bool, market_name: str = "") -> str:
    n = len(runners)
    sel_ids = {str(r.get("selectionId", "")) for r in runners}
    name_lower = (market_name or "").lower().strip()

    # Use market name for richer classification when available
    if name_lower:
        # Top finish MUST be checked BEFORE outright — "Top 5" contains "to win" in some variants
        if "top " in name_lower and ("nationality" in name_lower or "region" in name_lower or "country" in name_lower):
            return "top_nationality"
        if "top " in name_lower and any(x in name_lower for x in ("finish", "5", "10", "20", "40")):
            return "top_finish"
        if "outright" in name_lower or "to win" in name_lower or "winner" in name_lower:
            return "outright_winner"
        if "matchup" in name_lower or "match bet" in name_lower or "head to head" in name_lower or "h2h" in name_lower:
            return "matchup"
        if "3 ball" in name_lower or "3-ball" in name_lower or "three ball" in name_lower:
            return "three_ball"
        if "round" in name_lower and ("score" in name_lower or "leader" in name_lower):
            return "round_score"
        if "hole" in name_lower and ("score" in name_lower or "in one" in name_lower):
            return "hole_score"
        if "make" in name_lower and "cut" in name_lower:
            return "make_cut"
        if "first round leader" in name_lower:
            return "first_round_leader"
        if re.match(r"^group\s+[a-z0-9]", name_lower) or "golf specials" in name_lower:
            return "finishing_position"

    # Fallback to runner-count heuristic
    if n == 2:
        return "round_score" if sel_ids & ROUND_SCORE_SELECTION_IDS else "matchup"
    if n == 3:
        return "hole_score" if sel_ids & HOLE_SCORE_SELECTION_IDS else "three_ball"
    if n >= 4:
        # Without a market name, we can't distinguish outright from top-finish.
        # Use "unknown_large" so it doesn't silently merge with real outright data.
        if not name_lower:
            return "unknown_large"
        return "finishing_position" if not turn_in_play else "outright_winner"
    return "other"


def _american_from_decimal(decimal: float) -> str:
    if decimal <= 1.0:
        return "N/A"
    if decimal >= 2.0:
        return f"+{int(round((decimal - 1) * 100))}"
    return str(int(round(-100 / (decimal - 1))))


def _american_from_decimal_int(decimal: float) -> Optional[int]:
    """Return american odds as int to match BQ INTEGER column."""
    if decimal <= 1.0:
        return None
    if decimal >= 2.0:
        return int(round((decimal - 1) * 100))
    return int(round(-100 / (decimal - 1)))


def _build_deep_link(market_id: str, selection_id: str) -> str:
    params = urlencode([("marketId[]", market_id), ("selectionId[]", selection_id)])
    return f"fanduelsportsbook://launch?deepLink=addToBetslip%3F{params}"


# ---------------------------------------------------------------------------
# Parse getMarketPrices response
# ---------------------------------------------------------------------------

def _parse_market_prices_response(
    body: List[Any],
    scraped_at: str,
    event_name: str = "",
    player_map: Optional[Dict[str, str]] = None,
    market_name_map: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not isinstance(body, list):
        return rows

    player_map = player_map or {}
    market_name_map = market_name_map or {}
    raw_str = json.dumps(body)[:32000]

    for market in body:
        if not isinstance(market, dict):
            continue

        market_id = str(market.get("marketId", ""))
        turn_in_play = bool(market.get("turnInPlayEnabled", True))
        inplay = bool(market.get("inplay", False))
        market_status = market.get("marketStatus", "")
        # Prefer name from nav data (richer), fall back to getMarketPrices field
        market_name = (
            market_name_map.get(market_id)
            or market.get("marketName")
            or market.get("name")
            or ""
        )
        runners = market.get("runnerDetails", [])

        if not market_id or not runners:
            continue

        market_type = _classify_market(runners, turn_in_play, market_name)

        for runner in runners:
            if not isinstance(runner, dict):
                continue

            selection_id = str(runner.get("selectionId", ""))
            runner_name = runner.get("runnerName") or player_map.get(selection_id, "")
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
                    odds_am = int(am_val)
            except Exception:
                pass
            if odds_dec is not None and odds_am is None:
                odds_am = _american_from_decimal_int(odds_dec)

            deep_link = (
                _build_deep_link(market_id, selection_id)
                if market_id and selection_id else None
            )

            # Coerce IDs to numeric types to match BQ schema
            try:
                market_id_num = float(market_id) if market_id else None
            except (ValueError, TypeError):
                market_id_num = None
            try:
                selection_id_int = int(selection_id) if selection_id else None
            except (ValueError, TypeError):
                selection_id_int = None

            rows.append({
                "scraped_at": scraped_at,
                "source": "getMarketPrices",
                "event_name": event_name or None,
                "market_id": market_id_num,
                "market_name": market_name or None,
                "market_type": market_type,
                "market_status": market_status or None,
                "turn_in_play": turn_in_play,
                "inplay": inplay,
                "selection_id": selection_id_int,
                "player_name": runner_name or None,
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
    event_name: str = "",
    player_map: Optional[Dict[str, str]] = None,
    market_name_map: Optional[Dict[str, str]] = None,
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
                body, scraped_at, event_name, player_map, market_name_map
            )
            if not rows:
                body_type = type(body).__name__
                if isinstance(body, list):
                    preview = f"list(len={len(body)})"
                    if body:
                        preview += f" first={json.dumps(body[0])[:300]}"
                else:
                    preview = json.dumps(body)[:500] if not isinstance(body, str) else body[:500]
                logger.warning(
                    "  → 0 rows (status=%d type=%s body=%s)",
                    resp.status_code, body_type, preview,
                )
            else:
                logger.info("  → %d rows", len(rows))
            all_rows.extend(rows)
        except Exception as exc:
            logger.warning("Batch %d failed: %s", i + 1, exc)
        if i < len(batches) - 1:
            time.sleep(0.5)

    return all_rows


# ---------------------------------------------------------------------------
# Discover mode
# ---------------------------------------------------------------------------

def discover() -> None:
    """Load the golf landing page with a very broad capture and log all XHRs."""
    logger.info("DISCOVERY MODE → %s", FD_GOLF_URL)

    result = _call_proxy({
        "url": FD_GOLF_URL,
        "prime_url": FD_GOLF_URL,
        "capture_patterns": ["fanduel.com"],
        "wait_ms": 30000,
        "timeout_ms": 120000,
    })

    # Also extract tournaments from nav data
    for cap in result.get("captured_requests", []):
        if "content-managed-page" in cap.get("url", ""):
            nav_data = _try_parse_json(cap.get("body"))
            if nav_data:
                events = nav_data.get("attachments", {}).get("events", {})
                print(f"\n=== DISCOVERED {len(events)} TOURNAMENT(S) ===")
                for ev_id, ev in events.items():
                    if isinstance(ev, dict):
                        name = ev.get("name", "")
                        seo = ev.get("seoIdentifier", "")
                        slug = seo or _slug_from_name(name)
                        start = (ev.get("openDate") or "")[:10]
                        url = f"{FD_BASE_URL}/golf/{slug}-{ev_id}"
                        print(f"  {name:40s} → {url}  (starts {start})")
                print()

    captured = result.get("captured_requests", [])
    logger.info("page_status=%s  total_captured=%d", result.get("status"), len(captured))

    print("\n=== DISCOVERED XHRs ===")
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

def scrape(dry_run: bool = False) -> List[Dict[str, Any]]:
    scraped_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Step 1: Load golf landing page — discovers tournaments + per-event preview data
    tournaments, per_event_data, all_landing_ids, px_context = (
        _scrape_golf_landing()
    )

    if not tournaments:
        logger.error("No tournaments found. Run --discover to debug.")
        return []

    logger.info("Found %d golf tournament(s) to scrape", len(tournaments))

    all_rows: List[Dict[str, Any]] = []

    for ti, tourn in enumerate(tournaments):
        tourn_name = tourn["name"]
        tourn_slug = tourn["slug"]
        tourn_url = tourn["url"]
        tourn_id = tourn["id"]
        logger.info(
            "=== Tournament %d/%d: %s ===", ti + 1, len(tournaments), tourn_name,
        )

        # Use per-event data (landing page preview + full page data if loaded)
        evt_data = per_event_data.get(tourn_id)
        landing_count = len(evt_data["market_ids"]) if evt_data else 0

        if evt_data and landing_count > 0:
            market_ids = evt_data["market_ids"]
            player_map = evt_data["player_map"]
            market_name_map = evt_data["market_name_map"]
            event_name = tourn_name
            logger.info(
                "Using %d marketIds for event %s (id=%s)",
                len(market_ids), tourn_name, tourn_id,
            )

        if not market_ids:
            logger.warning("No marketIds for %s — skipping.", tourn_name)
            continue

        resolved_event_name = event_name or tourn_name

        logger.info(
            "Discovered %d marketIds for %r (%d players, %d market names, px=%s)",
            len(market_ids), resolved_event_name, len(player_map),
            len(market_name_map), "YES" if px_context else "NO",
        )

        rows = _fetch_market_prices(
            sorted(market_ids),
            scraped_at=scraped_at,
            event_name=resolved_event_name,
            player_map=player_map,
            market_name_map=market_name_map,
            px_context=px_context,
        )

        # Tag every row with tournament metadata for multi-event support
        for row in rows:
            row["tournament_name"] = resolved_event_name
            row["tournament_slug"] = tourn_slug

        by_type: Dict[str, int] = {}
        for row in rows:
            mt = row.get("market_type", "other")
            by_type[mt] = by_type.get(mt, 0) + 1

        logger.info(
            "%s: %d rows, %d market types", resolved_event_name, len(rows), len(by_type),
        )
        for mt, count in sorted(by_type.items(), key=lambda x: -x[1]):
            logger.info("  %-25s %d", mt, count)

        all_rows.extend(rows)

        # Longer pause between tournaments — PX needs time to cool down
        if ti < len(tournaments) - 1:
            logger.info("Waiting 10s before next tournament...")
            time.sleep(10)

    logger.info(
        "FanDuel PGA TOTAL: %d rows across %d tournament(s)", len(all_rows), len(tournaments),
    )

    if not all_rows:
        logger.warning("0 rows parsed across all tournaments.")
        return []

    if dry_run:
        seen: Set[str] = set()
        for row in all_rows:
            key = f"{row.get('tournament_name', '')}|{row.get('market_type', '')}"
            if key not in seen:
                seen.add(key)
                print(json.dumps(row, default=str))
        return all_rows

    Path(ARTIFACT_PATH).parent.mkdir(parents=True, exist_ok=True)
    with open(ARTIFACT_PATH, "w") as fh:
        for row in all_rows:
            fh.write(json.dumps(row, default=str) + "\n")
    logger.info("Wrote %d rows to %s", len(all_rows), ARTIFACT_PATH)
    return all_rows


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------

def load() -> None:
    from google.cloud import bigquery
    from google.cloud.bigquery import LoadJobConfig, SourceFormat
    import io

    if not Path(ARTIFACT_PATH).exists():
        logger.warning("No artifact at %s", ARTIFACT_PATH)
        return

    rows = []
    with open(ARTIFACT_PATH) as fh:
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
        autodetect=True,
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
        ],
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
    parser = argparse.ArgumentParser(description="FanDuel PGA Tour market scraper v5")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--scrape-only", action="store_true")
    group.add_argument("--load-only", action="store_true")
    group.add_argument("--discover", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if args.load_only:
        load()
    elif args.discover:
        discover()
    else:
        rows = scrape(dry_run=args.dry_run)
        if not args.scrape_only and not args.dry_run and rows:
            load()


if __name__ == "__main__":
    main()

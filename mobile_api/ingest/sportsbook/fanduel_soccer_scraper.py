"""
FanDuel Soccer Market Scraper — v2 (PGA-style architecture)

Architecture (mirrors fanduel_pga_scraper.py):
  1. Loads league page via Camoufox, captures content-managed-page + getMarketPrices
  2. Extracts all marketIds from nav response (layout coupons + attachments)
  3. Extracts event metadata (home_team, away_team, start) from attachments.events
  4. POSTs discovered marketIds to getMarketPrices to get full odds
  5. Joins event metadata onto market rows via eventId
  6. Writes to BigQuery table: oddspedia.raw_fanduel_soccer_markets

Usage:
  python -m mobile_api.ingest.sportsbook.fanduel_soccer_scraper --league EPL --dry-run
  python -m mobile_api.ingest.sportsbook.fanduel_soccer_scraper --league MLS --scrape-only
  python -m mobile_api.ingest.sportsbook.fanduel_soccer_scraper --league EPL
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

DATASET = "soccer_data"
TABLE = "raw_fanduel_soccer_markets"
ARTIFACT_PATTERN = "/tmp/fanduel_{league}_rows.ndjson"

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

CAPTURE_PATTERNS = ["content-managed-page", "getMarketPrices"]

# Load the sport-level /soccer page (uses content-managed-page + getMarketPrices).
# League-specific URLs (/soccer/mls, /soccer/premier-league) use a different
# facet/search API that returns empty data — so we load /soccer and filter by
# competition name from event metadata.
FD_SOCCER_URL = "https://sportsbook.fanduel.com/soccer"

# FanDuel competition IDs for filtering events by league.
# Found via discovery: attachments.competitions in content-managed-page response.
LEAGUE_COMPETITION_IDS: Dict[str, Set[str]] = {
    "EPL": {"10932509"},       # English Premier League
    "MLS": {"141"},            # US MLS
}

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


def _american_from_decimal(decimal: float) -> str:
    if decimal <= 1.0:
        return "N/A"
    if decimal >= 2.0:
        return f"+{int(round((decimal - 1) * 100))}"
    return str(int(round(-100 / (decimal - 1))))


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
    """Extract all externalMarketIds from layout coupons + attachments."""
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
        ext_id = market.get("externalMarketId")
        if ext_id:
            market_ids.add(str(ext_id))
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
        for assoc in market.get("associatedMarkets", []):
            if not isinstance(assoc, dict):
                continue
            aid = assoc.get("externalMarketId")
            aname = assoc.get("marketName") or assoc.get("name") or name
            if aid and aname:
                market_names[str(aid)] = aname
    return market_names


def _extract_market_event_map(data: Dict[str, Any]) -> Dict[str, str]:
    """Build externalMarketId → eventId from nav attachments.markets."""
    mapping: Dict[str, str] = {}
    for market_id, market in data.get("attachments", {}).get("markets", {}).items():
        if not isinstance(market, dict):
            continue
        event_id = str(market.get("eventId") or "")
        if not event_id:
            continue
        ext_id = str(market.get("externalMarketId") or market_id)
        if ext_id:
            mapping[ext_id] = event_id
        for assoc in market.get("associatedMarkets", []):
            if isinstance(assoc, dict):
                aid = str(assoc.get("externalMarketId") or "")
                if aid:
                    mapping[aid] = event_id
    return mapping


def _extract_events(data: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    Build eventId → {home_team, away_team, event_start, event_id} from nav
    attachments.events.
    """
    events: Dict[str, Dict[str, Any]] = {}
    for ev_id, ev in data.get("attachments", {}).get("events", {}).items():
        if not isinstance(ev, dict):
            continue
        name = ev.get("name") or ""
        home = ""
        away = ""
        # FanDuel names events like "Team A v Team B" or "Team A @ Team B"
        for sep in (" v ", " vs ", " @ "):
            if sep in name:
                parts = name.split(sep, 1)
                home = parts[0].strip()
                away = parts[1].strip()
                break
        if not home:
            home = name

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

        events[str(ev_id)] = {
            "event_id": str(ev_id),
            "home_team": home or None,
            "away_team": away or None,
            "event_start": event_start,
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
# Parse getMarketPrices response (soccer-specific)
# ---------------------------------------------------------------------------

def _parse_market_prices_response(
    body: List[Any],
    scraped_at: str,
    league: str,
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
    raw_str = json.dumps(body)[:32000]

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
        market_status = market.get("marketStatus", "")
        turn_in_play = bool(market.get("turnInPlayEnabled", False))
        inplay = bool(market.get("inplay", False))
        runners = market.get("runnerDetails", [])

        if not market_id or not runners:
            continue

        # Look up event metadata via market → event mapping
        event_id = market_event_map.get(market_id, "")
        event_meta = event_map.get(event_id, {})
        home_team = event_meta.get("home_team")
        away_team = event_meta.get("away_team")
        event_start = event_meta.get("event_start")

        for runner in runners:
            if not isinstance(runner, dict):
                continue

            selection_id = str(runner.get("selectionId", ""))
            selection_name = (
                runner.get("runnerName")
                or runner_name_map.get(selection_id, "")
            )
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

            # Determine outcome side
            name_lower = (selection_name or "").lower()
            if "draw" in name_lower or "tie" in name_lower:
                side = "draw"
            elif home_team and home_team.lower() in name_lower:
                side = "home"
            elif away_team and away_team.lower() in name_lower:
                side = "away"
            else:
                side = None

            deep_link = (
                _build_deep_link(market_id, selection_id)
                if market_id and selection_id else None
            )

            runner_status = runner.get("runnerStatus", "")

            rows.append({
                "scraped_at": scraped_at,
                "source": "getMarketPrices",
                "league": league,
                "event_id": event_id or None,
                "home_team": home_team,
                "away_team": away_team,
                "event_start": event_start,
                "market_id": market_id or None,
                "market_name": market_name or None,
                "market_type": market_name or None,
                "market_type_raw": market_name or None,
                "market_status": market_status or None,
                "turn_in_play": turn_in_play,
                "inplay": inplay,
                "selection_id": selection_id or None,
                "selection_name": selection_name or None,
                "runner_status": runner_status or None,
                "outcome_side": side,
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
    league: str,
    event_map: Dict[str, Dict[str, Any]],
    market_event_map: Dict[str, str],
    market_name_map: Optional[Dict[str, str]] = None,
    runner_name_map: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    all_rows: List[Dict[str, Any]] = []
    batches = [
        market_ids[i:i + MARKET_PRICES_BATCH_SIZE]
        for i in range(0, len(market_ids), MARKET_PRICES_BATCH_SIZE)
    ]

    headers = {**FD_API_HEADERS}

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
                body, scraped_at, league, event_map, market_event_map,
                market_name_map, runner_name_map,
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

def scrape(league: str, dry_run: bool = False) -> List[Dict[str, Any]]:
    league = league.upper()
    comp_ids = LEAGUE_COMPETITION_IDS.get(league)
    if comp_ids is None:
        raise ValueError(f"Unknown league '{league}'. Choose from: {list(LEAGUE_COMPETITION_IDS)}")

    scraped_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Phase 1: Load /soccer sport page via Camoufox.
    # League-specific URLs use facet/search which returns empty data.
    # The sport page uses content-managed-page with ALL soccer events.
    logger.info("Loading FanDuel Soccer via Camoufox: %s", FD_SOCCER_URL)
    result = _call_proxy({
        "url": FD_SOCCER_URL,
        "prime_url": "https://sportsbook.fanduel.com",
        "capture_patterns": CAPTURE_PATTERNS,
        "wait_ms": 40000,
        "timeout_ms": 120000,
    })

    page_status = result.get("status", 0)
    captured = result.get("captured_requests", [])
    logger.info("FanDuel Soccer: page_status=%s, captured=%d requests", page_status, len(captured))

    all_market_ids: Set[str] = set()
    market_name_map: Dict[str, str] = {}
    market_event_map: Dict[str, str] = {}
    all_event_map: Dict[str, Dict[str, Any]] = {}
    runner_name_map: Dict[str, str] = {}
    # Track competitionId per event for filtering
    event_competition: Dict[str, str] = {}

    for i, cap in enumerate(captured):
        cap_url = cap.get("url", "")
        cap_body = cap.get("body")
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

        # content-managed-page → nav JSON with events + markets
        if "content-managed-page" in cap_url:
            nav_data = _try_parse_json(cap_body)
            if not nav_data or ("layout" not in nav_data and "attachments" not in nav_data):
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

            # Extract competitionId per event for league filtering
            for ev_id, ev in nav_data.get("attachments", {}).get("events", {}).items():
                if isinstance(ev, dict) and ev.get("competitionId"):
                    event_competition[str(ev_id)] = str(ev["competitionId"])

            logger.info(
                "  → content-managed-page: %d marketIds, %d events, %d market names, %d runners",
                len(nav_ids), len(nav_events), len(nav_names), len(nav_runners),
            )

        # getMarketPrices — capture additional market IDs from responses
        if "getMarketPrices" in cap_url:
            if isinstance(cap_body, list):
                ids = [str(m["marketId"]) for m in cap_body if isinstance(m, dict) and m.get("marketId")]
                all_market_ids.update(ids)
                logger.info("  → getMarketPrices response: %d marketIds", len(ids))
            else:
                body_data = _try_parse_json(cap_body)
                if isinstance(body_data, dict) and "marketIds" in body_data:
                    ids = [str(m) for m in body_data["marketIds"] if m]
                    all_market_ids.update(ids)
                    logger.info("  → getMarketPrices POST body: %d marketIds", len(ids))

    # Filter to events belonging to the target league
    league_event_ids = {
        eid for eid, cid in event_competition.items() if cid in comp_ids
    }
    # Filter market IDs to only those linked to league events
    league_market_ids = {
        mid for mid, eid in market_event_map.items() if eid in league_event_ids
    }
    # Also keep markets from getMarketPrices captures that we can't filter
    # (they'll be filtered out in post-processing if no event match)
    event_map = {eid: all_event_map[eid] for eid in league_event_ids if eid in all_event_map}

    logger.info(
        "FanDuel %s: %d/%d events match competition filter, %d/%d marketIds, %d market names",
        league, len(league_event_ids), len(all_event_map),
        len(league_market_ids), len(all_market_ids), len(market_name_map),
    )

    if not league_market_ids:
        logger.warning(
            "FanDuel %s: 0 marketIds after competition filter (comp_ids=%s). "
            "Possible causes: no fixtures, or competition IDs changed.",
            league, comp_ids,
        )
        return []

    # Phase 2: POST to getMarketPrices for full odds
    rows = _fetch_market_prices(
        sorted(league_market_ids),
        scraped_at=scraped_at,
        league=league,
        event_map=event_map,
        market_event_map=market_event_map,
        market_name_map=market_name_map,
        runner_name_map=runner_name_map,
    )

    logger.info("FanDuel %s: %d total rows", league, len(rows))

    if not rows:
        logger.warning("FanDuel %s: 0 rows parsed from %d marketIds.", league, len(market_ids))

    if dry_run:
        for row in rows[:10]:
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
# Load phase
# ---------------------------------------------------------------------------

def load(league: str) -> None:
    from google.cloud import bigquery
    from google.cloud.bigquery import LoadJobConfig, SourceFormat
    import io

    artifact = ARTIFACT_PATTERN.format(league=league.lower())
    if not Path(artifact).exists():
        logger.warning("No artifact at %s — nothing to load", artifact)
        return

    rows = []
    with open(artifact) as fh:
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
        autodetect=True,
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
        ],
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

def discover(league: str) -> None:
    """Load the soccer page with broad capture patterns and log all XHRs."""
    logger.info("DISCOVERY MODE → %s (filtering for %s)", FD_SOCCER_URL, league)
    result = _call_proxy({
        "url": FD_SOCCER_URL,
        "prime_url": "https://sportsbook.fanduel.com",
        "capture_patterns": ["fanduel.com"],
        "wait_ms": 40000,
        "timeout_ms": 120000,
    })

    captured = result.get("captured_requests", [])
    logger.info("page_status=%s  total_captured=%d", result.get("status"), len(captured))

    print(f"\n=== DISCOVERED XHRs for {league} ===")
    for i, cap in enumerate(captured):
        url = cap.get("url", "<unknown>")
        body = cap.get("body")
        body_type = type(body).__name__
        top_keys = list(body.keys())[:6] if isinstance(body, dict) else "n/a"
        body_len = len(body) if isinstance(body, (str, list)) else "n/a"
        print(f"[{i:02d}] {url[:140]}")
        print(f"      body_type={body_type}  top_keys={top_keys}  len={body_len}")
    print("=== END ===\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="FanDuel soccer market scraper v2")
    parser.add_argument("--league", default="EPL", help="EPL or MLS")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--scrape-only", action="store_true")
    group.add_argument("--load-only", action="store_true")
    group.add_argument("--discover", action="store_true")
    parser.add_argument("--dry-run", action="store_true", help="Print rows, skip BQ write")
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

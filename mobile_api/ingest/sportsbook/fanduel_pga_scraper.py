"""
FanDuel PGA Tour Market Scraper — v5

Architecture:
  1. Camoufox loads the current PGA tournament page
  2. Captures ALL XHRs matching broad patterns (navigation + getMarketPrices)
  3. From captured requests:
       - Navigation response body  → all externalMarketIds + player names
       - getMarketPrices POST bodies → additional marketIds
       - Any request headers       → x-px-context token
  4. POSTs all discovered marketIds to getMarketPrices directly (with px token)
  5. Classifies markets, builds deep links, writes to BigQuery

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

FD_APP_CONTEXT_URL = (
    "https://api.sportsbook.fanduel.com/sbapi/application-context"
    "?dataEntries=POPULAR_BETTING,QUICK_LINKS,AZ_BETTING,EVENT_TYPES,TEASER_COMPS"
    "&_ak=FhMFpcPWXMeyZxOx"
)
FD_GOLF_EVENT_TYPE_ID = "3"

FD_MARKET_PRICES_URL = (
    "https://smp.nj.sportsbook.fanduel.com"
    "/api/sports/fixedodds/readonly/v1/getMarketPrices?priceHistory=0"
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
    "x-sportsbook-region": "NJ",
    "X-Application": "FhMFpcPWXMeyZxOx",
}

MARKET_PRICES_BATCH_SIZE = 50

# Capture patterns — short substrings that Camoufox matches against XHR URLs.
CAPTURE_PATTERNS = [
    "navigation/pga",
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
# Tournament URL discovery
# ---------------------------------------------------------------------------

def _discover_tournament_url() -> str:
    try:
        resp = requests.get(
            FD_APP_CONTEXT_URL,
            headers={**FD_API_HEADERS, "Accept": "application/json"},
            timeout=15,
        )
        resp.raise_for_status()
        events = resp.json().get("events") or {}
        best_event = None
        best_start = ""
        for ev_id, ev in events.items():
            if not isinstance(ev, dict):
                continue
            if str(ev.get("eventTypeId", "")) != FD_GOLF_EVENT_TYPE_ID:
                continue
            seo = ev.get("seoIdentifier") or ev.get("slug") or ""
            start = ev.get("openDate") or ev.get("startTime") or ""
            name = ev.get("eventName") or ev.get("name") or ""
            if seo and ev_id and (best_start == "" or start > best_start):
                best_start = start
                best_event = (ev_id, seo, name)
        if best_event:
            ev_id, seo, name = best_event
            url = f"{FD_BASE_URL}/golf/{seo}-{ev_id}"
            logger.info("Tournament URL: %s (%s)", url, name)
            return url
    except Exception as exc:
        logger.warning("Tournament URL discovery failed: %s", exc)
    return FD_GOLF_URL


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

def _scrape_tournament_page() -> Tuple[Set[str], Dict[str, str], str, str]:
    """
    Load the tournament page via Camoufox and extract everything from captures.

    Returns: (market_ids, player_map, event_name, px_context)
    """
    market_ids: Set[str] = set()
    player_map: Dict[str, str] = {}
    event_name = ""
    px_context = ""

    tournament_url = _discover_tournament_url()
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

        # Navigation XHR
        if "navigation/pga" in cap_url or "navigation/" in cap_url:
            nav_data = _try_parse_json(cap_body)
            if nav_data and ("layout" in nav_data or "attachments" in nav_data):
                nav_ids = _extract_market_ids_from_nav(nav_data)
                nav_players = _extract_player_names_from_nav(nav_data)
                market_ids.update(nav_ids)
                player_map.update(nav_players)
                if not event_name:
                    event_name = _extract_event_name_from_nav(nav_data)
                logger.info("  → nav: %d marketIds, %d players", len(nav_ids), len(nav_players))

        # getMarketPrices POST body → market IDs
        if "getMarketPrices" in cap_url:
            body_data = _try_parse_json(cap_body)
            if isinstance(body_data, dict) and "marketIds" in body_data:
                ids = [str(m) for m in body_data["marketIds"] if m]
                market_ids.update(ids)
                logger.info("  → getMarketPrices POST: %d marketIds", len(ids))
            elif isinstance(cap_body, list):
                for mkt in cap_body:
                    if isinstance(mkt, dict) and mkt.get("marketId"):
                        market_ids.add(str(mkt["marketId"]))

    if not market_ids:
        logger.warning(
            "0 marketIds from %d captured requests. "
            "Run --discover (with capture_patterns=[\"fanduel.com\"]) to see all XHRs.",
            len(captured),
        )

    logger.info(
        "Result: %d marketIds, %d players, event=%r, px=%s",
        len(market_ids), len(player_map), event_name, "YES" if px_context else "NO",
    )
    return market_ids, player_map, event_name, px_context


# ---------------------------------------------------------------------------
# Market classification
# ---------------------------------------------------------------------------

def _classify_market(runners: List[Dict], turn_in_play: bool) -> str:
    n = len(runners)
    sel_ids = {str(r.get("selectionId", "")) for r in runners}
    if n == 2:
        return "round_score" if sel_ids & ROUND_SCORE_SELECTION_IDS else "matchup"
    if n == 3:
        return "hole_score" if sel_ids & HOLE_SCORE_SELECTION_IDS else "three_ball"
    if n >= 4:
        return "finishing_position" if not turn_in_play else "outright_winner"
    return "other"


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
    event_name: str = "",
    player_map: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not isinstance(body, list):
        return rows

    player_map = player_map or {}
    raw_str = json.dumps(body)[:32000]

    for market in body:
        if not isinstance(market, dict):
            continue

        market_id = str(market.get("marketId", ""))
        turn_in_play = bool(market.get("turnInPlayEnabled", True))
        inplay = bool(market.get("inplay", False))
        market_status = market.get("marketStatus", "")
        market_name = market.get("marketName") or market.get("name") or ""
        runners = market.get("runnerDetails", [])

        if not market_id or not runners:
            continue

        market_type = _classify_market(runners, turn_in_play)

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
                "event_name": event_name or None,
                "market_id": market_id,
                "market_name": market_name or None,
                "market_type": market_type,
                "market_status": market_status or None,
                "turn_in_play": turn_in_play,
                "inplay": inplay,
                "selection_id": selection_id or None,
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
            rows = _parse_market_prices_response(body, scraped_at, event_name, player_map)
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
    """Load the tournament page with a very broad capture and log all XHRs."""
    tournament_url = _discover_tournament_url()
    logger.info("DISCOVERY MODE → %s", tournament_url)

    result = _call_proxy({
        "url": tournament_url,
        "prime_url": tournament_url,
        "capture_patterns": ["fanduel.com"],  # catch everything
        "wait_ms": 30000,
        "timeout_ms": 120000,
    })

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

    market_ids, player_map, event_name, px_context = _scrape_tournament_page()

    if not market_ids:
        logger.error("No marketIds found. Run --discover to debug XHR capture.")
        return []

    logger.info(
        "Discovered %d marketIds for %r (%d players, px=%s)",
        len(market_ids), event_name, len(player_map), "YES" if px_context else "NO",
    )

    rows = _fetch_market_prices(
        sorted(market_ids),
        scraped_at=scraped_at,
        event_name=event_name,
        player_map=player_map,
        px_context=px_context,
    )

    by_type: Dict[str, int] = {}
    for row in rows:
        mt = row.get("market_type", "other")
        by_type[mt] = by_type.get(mt, 0) + 1

    logger.info("FanDuel PGA: %d rows, %d market types", len(rows), len(by_type))
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

    Path(ARTIFACT_PATH).parent.mkdir(parents=True, exist_ok=True)
    with open(ARTIFACT_PATH, "w") as fh:
        for row in rows:
            fh.write(json.dumps(row, default=str) + "\n")
    logger.info("Wrote %d rows to %s", len(rows), ARTIFACT_PATH)
    return rows


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
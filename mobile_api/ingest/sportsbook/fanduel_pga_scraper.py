"""
FanDuel PGA Tour Market Scraper.

Calls the Camoufox Cloud Run proxy to load FanDuel golf pages with real
browser TLS fingerprinting, captures internal API responses, parses
marketId + selectionId values for FanDuel deep links, and writes rows to
BigQuery table: sportsbook.raw_fanduel_pga_markets

Captures all available market types:
  - Tournament winner / outright
  - Round leader
  - Top 5 / Top 10 / Top 20 finishes
  - Make/miss cut
  - Head-to-head matchups
  - Stroke props (over/under)

Usage:
  # Discovery mode — logs ALL XHR URLs fired by the page
  python -m mobile_api.ingest.sportsbook.fanduel_pga_scraper --discover

  python -m mobile_api.ingest.sportsbook.fanduel_pga_scraper --scrape-only
  python -m mobile_api.ingest.sportsbook.fanduel_pga_scraper --dry-run
  python -m mobile_api.ingest.sportsbook.fanduel_pga_scraper --load-only

Fix notes (2026-03-28 v3 — post-discovery):
  Discovery run confirmed Camoufox is working (64 captured requests).
  Correct API domains identified:
    [50] api.sportsbook.fanduel.com/sbapi/content-managed-page  — golf layout+attachments
    [54-63] smp.ia.sportsbook.fanduel.com/.../getMarketPrices  — live odds polling
  Previous patterns (sbapi.fanduel.com, sportsbook-nash.fanduel.com) were
  completely wrong domains. Updated to the real ones.

  Parser updated for the actual response shapes:
    content-managed-page → body["attachments"]["events"] (dict keyed by eventId)
                         → body["attachments"]["markets"] (dict keyed by marketId)
                         → body["attachments"]["runners"] (dict keyed by runnerId)
    getMarketPrices      → list of price objects with marketId, selectionId, price
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
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

FD_APP_CONTEXT_URL = (
    "https://api.sportsbook.fanduel.com/sbapi/application-context"
    "?dataEntries=EVENT_TYPES&_ak=FhMFpcPWXMeyZxOx"
)
FD_GOLF_EVENT_TYPE_ID = "3"  # Golf event type ID confirmed from discovery
FD_BASE_URL = "https://sportsbook.fanduel.com"


def _discover_tournament_url() -> str:
    """
    Dynamically find the current PGA Tour tournament URL from FanDuel's
    application-context API. Returns tournament URL like:
      https://sportsbook.fanduel.com/golf/texas-children%27s-houston-open-35406067
    Falls back to the generic golf hub if discovery fails.
    """
    fallback = "https://sportsbook.fanduel.com/golf"
    try:
        resp = requests.get(
            FD_APP_CONTEXT_URL,
            headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()

        # EVENT_TYPES contains sport nav links including current golf events
        event_types = data.get("EVENT_TYPES") or {}
        events = data.get("events") or {}

        # Look for golf (eventTypeId=3) competitions with active events
        # The layout links contain competitionId → look up event names
        layout = event_types.get("layout") or {}
        links = layout.get("links") or {}

        # Also check attachments/coupons for active golf events
        attachments = event_types.get("attachments") or {}
        competitions = attachments.get("competitions") or {}

        # Find the most recently started golf event
        best_event = None
        best_start = None
        for ev_id, ev in events.items():
            if not isinstance(ev, dict):
                continue
            if str(ev.get("eventTypeId", "")) != FD_GOLF_EVENT_TYPE_ID:
                continue
            seo = ev.get("seoIdentifier") or ev.get("slug") or ""
            start = ev.get("openDate") or ev.get("startTime") or ""
            name = ev.get("eventName") or ev.get("name") or ""
            if seo and ev_id:
                if best_start is None or start > best_start:
                    best_start = start
                    best_event = (ev_id, seo, name)

        if best_event:
            ev_id, seo, name = best_event
            url = f"{FD_BASE_URL}/golf/{seo}-{ev_id}"
            logger.info("FanDuel PGA: current tournament → %s (%s)", name, url)
            return url

        # Fallback: check coupons in the content-managed-page layout for golf events
        logger.warning("FanDuel PGA: no current golf event found in application-context — using fallback")
    except Exception as exc:
        logger.warning("FanDuel PGA: tournament discovery failed: %s — using fallback", exc)

    return fallback
    "url": "https://sportsbook.fanduel.com/golf",
    "prime_url": "https://sportsbook.fanduel.com",
    # Confirmed from discovery run [50], [54-63]
    "capture_patterns": [
        "api.sportsbook.fanduel.com/sbapi/content-managed-page",
        "smp.ia.sportsbook.fanduel.com/api/sports/fixedodds",
    ],
    # 25s — getMarketPrices polls repeatedly; we want several cycles
    "wait_ms": 25000,
}

SCRAPE_CONFIG = {
    "url": "https://sportsbook.fanduel.com/golf",
    "prime_url": "https://sportsbook.fanduel.com",
    "capture_patterns": [
        "api.sportsbook.fanduel.com/sbapi/content-managed-page",
        "smp.ia.sportsbook.fanduel.com/api/sports/fixedodds",
    ],
    "wait_ms": 25000,
}


DISCOVER_PATTERNS = [
    "fanduel.com",
    "api.",
    "smp.",
]


# ---------------------------------------------------------------------------
# Proxy helpers
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
        timeout=120,
    )
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# Discovery mode
# ---------------------------------------------------------------------------

def discover() -> None:
    tournament_url = _discover_tournament_url()
    logger.info("DISCOVERY MODE — capturing all XHRs from %s", tournament_url)
    result = _call_proxy({
        "url": tournament_url,
        "prime_url": tournament_url,
        "capture_patterns": DISCOVER_PATTERNS,
        "wait_ms": SCRAPE_CONFIG["wait_ms"],
        "timeout_ms": 90000,
    })
    captured = result.get("captured_requests", [])
    logger.info("page_status=%d  total_captured=%d", result.get("status", 0), len(captured))
    print("\n=== DISCOVERED URLs ===")
    for i, capture in enumerate(captured):
        url = capture.get("url", "<unknown>")
        body = capture.get("body")
        body_type = type(body).__name__
        top_keys = list(body.keys())[:6] if isinstance(body, dict) else "n/a"
        print(f"[{i:02d}] {url}")
        print(f"      body_type={body_type}  top_keys={top_keys}  is_list={isinstance(body, list)}")
    print("=== END DISCOVERED URLs ===\n")


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
    params = urlencode(
        [("marketId[]", market_id), ("selectionId[]", selection_id)]
    )
    return f"fanduelsportsbook://launch?deepLink=addToBetslip%3F{params}"


# ---------------------------------------------------------------------------
# Parsing — content-managed-page (attachments style)
# ---------------------------------------------------------------------------

def _classify_market(market_name: str) -> str:
    n = market_name.lower()
    if any(x in n for x in ["winner", "outright", "tournament winner"]):
        return "outright_winner"
    if "round leader" in n:
        return "round_leader"
    if any(x in n for x in ["top 5", "top5"]):
        return "top_5"
    if any(x in n for x in ["top 10", "top10"]):
        return "top_10"
    if any(x in n for x in ["top 20", "top20"]):
        return "top_20"
    if "cut" in n:
        return "make_cut"
    if any(x in n for x in ["matchup", "head", "h2h", " vs "]):
        return "matchup"
    if any(x in n for x in ["stroke", "score", "over", "under", "total", "prop"]):
        return "stroke_prop"
    return "other"


def _parse_content_managed_page(body: Dict[str, Any], scraped_at: str) -> List[Dict[str, Any]]:
    """
    Parse api.sportsbook.fanduel.com/sbapi/content-managed-page response.

    Structure confirmed from discovery [50]:
      body["attachments"]["events"]  → dict keyed by eventId
      body["attachments"]["markets"] → dict keyed by marketId
      body["attachments"]["runners"] → dict keyed by runnerId (selections)
    """
    rows: List[Dict[str, Any]] = []
    raw_str = json.dumps(body)[:64000]

    attachments = body.get("attachments") or {}

    # Events dict
    events_dict: Dict[str, Any] = attachments.get("events") or {}
    if isinstance(events_dict, list):
        events_dict = {str(e.get("eventId", i)): e for i, e in enumerate(events_dict)}

    # Markets dict
    markets_dict: Dict[str, Any] = attachments.get("markets") or {}
    if isinstance(markets_dict, list):
        markets_dict = {str(m.get("marketId", i)): m for i, m in enumerate(markets_dict)}

    # Runners dict (selections)
    runners_dict: Dict[str, Any] = attachments.get("runners") or {}
    if isinstance(runners_dict, list):
        runners_dict = {str(r.get("selectionId", i)): r for i, r in enumerate(runners_dict)}

    if not markets_dict:
        logger.debug("content-managed-page: no markets in attachments")
        return rows

    for market_id, market in markets_dict.items():
        if not isinstance(market, dict):
            continue

        market_name = market.get("marketType") or market.get("name") or ""
        market_type = _classify_market(market_name)

        # Link back to event
        event_id = str(market.get("eventId") or "")
        event = events_dict.get(event_id, {})
        event_name = event.get("name") or event.get("eventName") or ""
        start_raw = event.get("openDate") or event.get("startDate") or ""
        try:
            event_start = (
                datetime.fromisoformat(start_raw.replace("Z", "+00:00")).strftime("%Y-%m-%dT%H:%M:%S")
                if start_raw else None
            )
        except Exception:
            event_start = None

        # Runners for this market
        market_runner_ids = market.get("runners") or market.get("selections") or []
        # runners may be a list of IDs or a list of dicts
        for runner_ref in market_runner_ids:
            if isinstance(runner_ref, dict):
                selection_id = str(runner_ref.get("selectionId") or runner_ref.get("id") or "")
                runner = runner_ref
            else:
                selection_id = str(runner_ref)
                runner = runners_dict.get(selection_id, {})

            if not isinstance(runner, dict):
                continue

            selection_name = runner.get("runnerName") or runner.get("name") or ""
            handicap = runner.get("handicap") or runner.get("line")
            try:
                handicap_f = float(handicap) if handicap is not None else None
            except Exception:
                handicap_f = None

            price = runner.get("currentPrice") or runner.get("price") or {}
            odds_dec = None
            odds_am = None
            if isinstance(price, dict):
                dec = price.get("d") or price.get("decimal") or price.get("decimalOdds")
                if dec is not None:
                    try:
                        odds_dec = float(dec)
                        odds_am = _american_from_decimal(odds_dec)
                    except Exception:
                        pass
            elif isinstance(price, (int, float)):
                odds_dec = float(price)
                odds_am = _american_from_decimal(odds_dec)

            deep_link = (
                _build_deep_link(market_id, selection_id)
                if market_id and selection_id else None
            )

            rows.append({
                "scraped_at": scraped_at,
                "source": "content-managed-page",
                "event_id": event_id or None,
                "event_name": event_name or None,
                "event_start": event_start,
                "market_id": market_id or None,
                "market_name": market_name or None,
                "market_type": market_type,
                "selection_id": selection_id or None,
                "selection_name": selection_name or None,
                "handicap": handicap_f,
                "odds_decimal": odds_dec,
                "odds_american": odds_am,
                "deep_link": deep_link,
                "raw_response": raw_str,
            })

    return rows


def _extract_market_prices_lookup(body: List[Any]) -> Dict[str, Any]:
    """
    Build a lookup dict: (marketId, str(selectionId)) -> {odds_decimal, odds_american}

    getMarketPrices response structure (confirmed from debug logs):
      body = list of market objects:
        {
          marketId: "719.160971940",
          runnerDetails: [
            {
              selectionId: 13496403,
              winRunnerOdds: {
                trueOdds: {
                  decimalOdds: { decimalOdds: 3.2 }
                },
                americanDisplayOdds: { americanOdds: 220.0 }
              }
            }, ...
          ]
        }
    """
    lookup: Dict[str, Any] = {}
    if not isinstance(body, list):
        return lookup

    for market in body:
        if not isinstance(market, dict):
            continue
        market_id = str(market.get("marketId", ""))
        for runner in market.get("runnerDetails", []):
            if not isinstance(runner, dict):
                continue
            sel_id = str(runner.get("selectionId", ""))
            if not sel_id:
                continue

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

            # Derive american from decimal if missing
            if odds_dec is not None and odds_am is None:
                odds_am = _american_from_decimal(odds_dec)

            key = (market_id, sel_id)
            # Keep first occurrence (earliest poll)
            if key not in lookup:
                lookup[key] = {"odds_decimal": odds_dec, "odds_american": odds_am}

    return lookup


def _parse_captured(captured: List[Dict[str, Any]], scraped_at: str) -> List[Dict[str, Any]]:
    """
    Two-pass parse:
      Pass 1 — collect all getMarketPrices responses into a (marketId, selectionId) odds lookup
      Pass 2 — parse content-managed-page rows, enrich with odds from lookup,
                filter out TOP_GROUP_IMG and rows with no odds
    """
    # Pass 1: build odds lookup from all getMarketPrices captures
    odds_lookup: Dict[tuple, Any] = {}
    layout_body = None

    for i, capture in enumerate(captured):
        url = capture.get("url", "<unknown>")
        body = capture.get("body")
        logger.info("  captured[%d]: %s", i, url)

        if "getMarketPrices" in url and isinstance(body, list):
            batch = _extract_market_prices_lookup(body)
            new_count = sum(1 for k in batch if k not in odds_lookup)
            odds_lookup.update({k: v for k, v in batch.items() if k not in odds_lookup})
            logger.info("  → getMarketPrices: +%d new odds entries (total=%d)", new_count, len(odds_lookup))
        elif "content-managed-page" in url and isinstance(body, dict):
            layout_body = body
            logger.info("  → content-managed-page: captured layout body")

    logger.info("FanDuel PGA: odds lookup has %d (marketId, selectionId) entries", len(odds_lookup))

    # Debug: log sample marketIds from the odds lookup so we can compare to layout
    sample_keys = list(odds_lookup.keys())[:5]
    for k in sample_keys:
        logger.info("  odds_lookup sample: marketId=%s selectionId=%s odds=%s", k[0], k[1], odds_lookup[k])

    if layout_body is None:
        logger.warning("FanDuel PGA: no content-managed-page body captured")
        return []

    # Pass 2: parse layout rows and enrich with odds
    raw_rows = _parse_content_managed_page(layout_body, scraped_at)
    logger.info("FanDuel PGA: content-managed-page produced %d raw rows", len(raw_rows))

    # Debug: log sample market_names and market_ids from layout rows
    seen_names = {}
    for r in raw_rows:
        mn = r.get("market_name", "")
        if mn not in seen_names:
            seen_names[mn] = r.get("market_id", "")
    for mn, mid in list(seen_names.items())[:10]:
        logger.info("  layout market_name=%s  market_id=%s", mn, mid)

    enriched: List[Dict[str, Any]] = []
    skipped_img = 0
    skipped_no_odds = 0

    for row in raw_rows:
        # Filter banner/image placeholder markets
        if row.get("market_name") == "TOP_GROUP_IMG":
            skipped_img += 1
            continue

        market_id = row.get("market_id") or ""
        selection_id = row.get("selection_id") or ""
        key = (market_id, selection_id)

        if key in odds_lookup:
            row["odds_decimal"] = odds_lookup[key]["odds_decimal"]
            row["odds_american"] = odds_lookup[key]["odds_american"]

        # Option A: strict — only keep rows with actual odds
        if row.get("odds_decimal") is None:
            skipped_no_odds += 1
            continue

        enriched.append(row)

    logger.info(
        "FanDuel PGA: %d captured, %d enriched rows (skipped %d TOP_GROUP_IMG, %d no-odds)",
        len(captured), len(enriched), skipped_img, skipped_no_odds,
    )
    return enriched


# ---------------------------------------------------------------------------
# Scrape
# ---------------------------------------------------------------------------

def scrape(dry_run: bool = False) -> List[Dict[str, Any]]:
    cfg = SCRAPE_CONFIG

    # Dynamically find the current tournament URL
    tournament_url = _discover_tournament_url()
    logger.info("Calling Camoufox proxy for FanDuel PGA → %s", tournament_url)

    result = _call_proxy({
        "url": tournament_url,
        "prime_url": tournament_url,
        "capture_patterns": cfg["capture_patterns"],
        "wait_ms": cfg["wait_ms"],
        "timeout_ms": 90000,
    })

    captured = result.get("captured_requests", [])
    logger.info(
        "FanDuel PGA: page_status=%d  captured_requests=%d",
        result.get("status", 0), len(captured),
    )

    scraped_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows = _parse_captured(captured, scraped_at=scraped_at)

    if not rows:
        logger.warning(
            "FanDuel PGA: 0 rows. Run --discover to check current XHR URLs."
        )

    logger.info("Parsed %d total rows for FanDuel PGA", len(rows))

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
# Load
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
    parser = argparse.ArgumentParser(description="FanDuel PGA Tour market scraper")
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
        if not args.scrape_only and not args.dry_run:
            load()


if __name__ == "__main__":
    main()
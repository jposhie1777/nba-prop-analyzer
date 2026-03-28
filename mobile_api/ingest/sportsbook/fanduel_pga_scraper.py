"""
FanDuel PGA Tour Market Scraper.

Calls the Camoufox Cloud Run proxy to load FanDuel golf pages with real
browser TLS fingerprinting, captures internal API responses, parses
marketId + selectionId values for FanDuel deep links, and writes rows to
BigQuery table: sportsbook.raw_fanduel_pga_markets

Usage:
  # Discovery mode — logs ALL XHR URLs fired by the page, no parsing
  python -m mobile_api.ingest.sportsbook.fanduel_pga_scraper --discover

  # Normal scrape
  python -m mobile_api.ingest.sportsbook.fanduel_pga_scraper --scrape-only
  python -m mobile_api.ingest.sportsbook.fanduel_pga_scraper --dry-run
  python -m mobile_api.ingest.sportsbook.fanduel_pga_scraper --load-only
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

SCRAPE_CONFIG = {
    "url": "https://sportsbook.fanduel.com/golf",
    "prime_url": "https://sportsbook.fanduel.com",
    # UPDATE THESE after running --discover and reviewing the URL log.
    # Current patterns are placeholders — 0 captures confirmed they're wrong.
    "capture_patterns": [
        "sbapi.fanduel.com/api/",
        "sportsbook-nash.fanduel.com/api/",
    ],
    "wait_ms": 25000,
}

# Discovery mode uses a broad pattern to catch everything
DISCOVER_PATTERNS = [
    "fanduel.com",
    "sbapi.",
    "sportsbook-nash.",
    "api.",
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
    """
    Load the FanDuel golf page with broad capture patterns and print every
    intercepted URL + top-level response keys. Use this output to identify
    the correct capture_patterns for the scrape config.
    """
    logger.info("DISCOVERY MODE — capturing all XHRs from %s", SCRAPE_CONFIG["url"])
    result = _call_proxy({
        "url": SCRAPE_CONFIG["url"],
        "prime_url": SCRAPE_CONFIG["prime_url"],
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
        body_is_list = isinstance(body, list)
        print(f"[{i:02d}] {url}")
        print(f"      body_type={body_type}  top_keys={top_keys}  is_list={body_is_list}")
    print("=== END DISCOVERED URLs ===\n")
    print("ACTION: Update capture_patterns in SCRAPE_CONFIG with the paths")
    print("        of URLs that look like odds/events/markets responses.")


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
# Response filter
# ---------------------------------------------------------------------------

def _is_fanduel_odds_response(body: Any) -> bool:
    if not isinstance(body, dict):
        return False
    if "facets" in body and not body.get("results"):
        return False
    events = (
        body.get("events")
        or body.get("data", {}).get("events")
        or body.get("result", {}).get("events")
        or (body.get("attachments") or {}).get("events")
        or body.get("results")
        or []
    )
    if isinstance(events, dict):
        return len(events) > 0
    if isinstance(events, list):
        return len(events) > 0
    return False


# ---------------------------------------------------------------------------
# Parsing
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


def _parse_body(body: Dict[str, Any], scraped_at: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    raw_str = json.dumps(body)[:64000]

    if isinstance(body, list):
        events = body
    else:
        events = (
            body.get("events")
            or body.get("data", {}).get("events")
            or body.get("result", {}).get("events")
            or []
        )
        attachments_events = (body.get("attachments") or {}).get("events", {})
        if not events and isinstance(attachments_events, dict):
            events = list(attachments_events.values())
        if not events and body.get("results"):
            events = body["results"]
        if not events and isinstance(body.get("data"), list):
            events = body["data"]

    if not events:
        return rows

    for event in events:
        if not isinstance(event, dict):
            continue

        event_id = str(event.get("eventId") or event.get("id") or event.get("event_id") or "")
        event_name = event.get("name") or event.get("eventName") or ""
        start_raw = event.get("openDate") or event.get("startDate") or event.get("startTime") or ""
        try:
            event_start = (
                datetime.fromisoformat(start_raw.replace("Z", "+00:00")).strftime("%Y-%m-%dT%H:%M:%S")
                if start_raw else None
            )
        except Exception:
            event_start = None

        markets = event.get("markets") or event.get("marketGroups") or []
        for market in markets:
            if not isinstance(market, dict):
                continue

            market_id = str(market.get("marketId") or market.get("id") or "")
            market_name = market.get("marketType") or market.get("name") or ""
            market_type = _classify_market(market_name)

            runners = (
                market.get("runners")
                or market.get("selections")
                or market.get("outcomes")
                or []
            )
            for runner in runners:
                if not isinstance(runner, dict):
                    continue

                selection_id = str(runner.get("selectionId") or runner.get("id") or "")
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


def _parse_captured(captured: List[Dict[str, Any]], scraped_at: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    odds_hits = 0
    for i, capture in enumerate(captured):
        url = capture.get("url", "<unknown>")
        body = capture.get("body")
        logger.info("  captured[%d]: %s", i, url)
        if not _is_fanduel_odds_response(body):
            logger.debug("  → skipping")
            continue
        odds_hits += 1
        rows.extend(_parse_body(body, scraped_at=scraped_at))
    logger.info(
        "FanDuel PGA: %d captured, %d odds responses, %d rows parsed",
        len(captured), odds_hits, len(rows),
    )
    return rows


# ---------------------------------------------------------------------------
# Scrape
# ---------------------------------------------------------------------------

def scrape(dry_run: bool = False) -> List[Dict[str, Any]]:
    cfg = SCRAPE_CONFIG
    logger.info("Calling Camoufox proxy for FanDuel PGA ...")
    result = _call_proxy({
        "url": cfg["url"],
        "prime_url": cfg["prime_url"],
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
            "FanDuel PGA: 0 rows. Run --discover to see all XHR URLs and "
            "update capture_patterns in SCRAPE_CONFIG."
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
    group.add_argument("--discover", action="store_true",
                       help="Log all XHR URLs to identify correct capture_patterns")
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
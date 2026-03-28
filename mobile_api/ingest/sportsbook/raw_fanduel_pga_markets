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

Usage (called by sportsbook_markets.yml workflow):

  python -m mobile_api.ingest.sportsbook.fanduel_pga_scraper --scrape-only
  python -m mobile_api.ingest.sportsbook.fanduel_pga_scraper --load-only

Environment variables required:
  CAMOUFOX_SERVICE_URL  - Cloud Run URL
  CAMOUFOX_TOKEN        - GCP identity token
  GCP_PROJECT           - GCP project ID
  GOOGLE_APPLICATION_CREDENTIALS - path to service account JSON (load phase)
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

# FanDuel golf page — covers the current PGA Tour event.
# FanDuel consolidates all active golf markets under /golf.
SCRAPE_CONFIG = {
    "url": "https://sportsbook.fanduel.com/golf",
    "prime_url": "https://sportsbook.fanduel.com",
    "capture_patterns": [
        "sbapi.fanduel.com",
        "sportsbook-nash.fanduel.com",
        "api.fanduel.com",
    ],
    # Golf pages load outrights + props in multiple sequential XHRs;
    # 20s gives all of them time to fire.
    "wait_ms": 20000,
    # Fallback: hit the FanDuel golf content API directly via proxy
    "fallback_url": (
        "https://sbapi.fanduel.com/api/content-managed-page"
        "?page=SPORT&eventTypeId=18&_ak=FhMFpcPWXMeyZxOx&tab=featured"
    ),
}


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
    """Reject empty facets/browse blobs; accept real events/markets payloads."""
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

def _parse_body(body: Dict[str, Any], scraped_at: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    raw_str = json.dumps(body)[:64000]

    # Normalise to a list of events
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

        event_id = str(
            event.get("eventId") or event.get("id") or event.get("event_id") or ""
        )
        event_name = event.get("name") or event.get("eventName") or ""
        start_raw = (
            event.get("openDate")
            or event.get("startDate")
            or event.get("startTime")
            or ""
        )
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

        markets = event.get("markets") or event.get("marketGroups") or []
        for market in markets:
            if not isinstance(market, dict):
                continue

            market_id = str(market.get("marketId") or market.get("id") or "")
            market_name = market.get("marketType") or market.get("name") or ""

            # Classify market type for easier downstream filtering
            name_lower = market_name.lower()
            if any(x in name_lower for x in ["winner", "outright", "tournament winner"]):
                market_type = "outright_winner"
            elif "round leader" in name_lower:
                market_type = "round_leader"
            elif any(x in name_lower for x in ["top 5", "top5"]):
                market_type = "top_5"
            elif any(x in name_lower for x in ["top 10", "top10"]):
                market_type = "top_10"
            elif any(x in name_lower for x in ["top 20", "top20"]):
                market_type = "top_20"
            elif "cut" in name_lower:
                market_type = "make_cut"
            elif any(x in name_lower for x in ["matchup", "head", "h2h", "vs"]):
                market_type = "matchup"
            elif any(x in name_lower for x in ["stroke", "score", "over", "under", "total"]):
                market_type = "stroke_prop"
            else:
                market_type = "other"

            runners = (
                market.get("runners")
                or market.get("selections")
                or market.get("outcomes")
                or []
            )
            for runner in runners:
                if not isinstance(runner, dict):
                    continue

                selection_id = str(
                    runner.get("selectionId") or runner.get("id") or ""
                )
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
                    dec = (
                        price.get("d")
                        or price.get("decimal")
                        or price.get("decimalOdds")
                    )
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
                    if market_id and selection_id
                    else None
                )

                rows.append(
                    {
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
                    }
                )

    return rows


def _parse_captured(
    captured: List[Dict[str, Any]], scraped_at: str
) -> List[Dict[str, Any]]:
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
# Fallback via proxy
# ---------------------------------------------------------------------------

def _fetch_via_proxy(target_url: str) -> Optional[Dict[str, Any]]:
    logger.info("FanDuel PGA: proxy fallback → %s", target_url)
    try:
        result = _call_proxy(
            {
                "url": target_url,
                "prime_url": SCRAPE_CONFIG["prime_url"],
                "capture_patterns": [],
                "wait_ms": 3000,
                "timeout_ms": 30000,
                "return_body": True,
            }
        )
        body = result.get("body") or result.get("page_body")
        if isinstance(body, str):
            try:
                body = json.loads(body)
            except json.JSONDecodeError:
                return None
        return body if isinstance(body, dict) else None
    except Exception as exc:
        logger.warning("FanDuel PGA: proxy fallback failed: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Scrape
# ---------------------------------------------------------------------------

def scrape(dry_run: bool = False) -> List[Dict[str, Any]]:
    cfg = SCRAPE_CONFIG
    logger.info("Calling Camoufox proxy for FanDuel PGA ...")
    result = _call_proxy(
        {
            "url": cfg["url"],
            "prime_url": cfg["prime_url"],
            "capture_patterns": cfg["capture_patterns"],
            "wait_ms": cfg["wait_ms"],
            "timeout_ms": 60000,
        }
    )

    captured = result.get("captured_requests", [])
    logger.info(
        "FanDuel PGA: page_status=%d  captured_requests=%d",
        result.get("status", 0),
        len(captured),
    )

    scraped_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows = _parse_captured(captured, scraped_at=scraped_at)

    if not rows and cfg.get("fallback_url"):
        logger.info("FanDuel PGA: 0 rows from browser capture — trying proxy fallback")
        body = _fetch_via_proxy(cfg["fallback_url"])
        if body and _is_fanduel_odds_response(body):
            rows = _parse_body(body, scraped_at=scraped_at)
            logger.info("FanDuel PGA: proxy fallback returned %d rows", len(rows))
        else:
            logger.warning("FanDuel PGA: proxy fallback returned nothing")

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
    job = client.load_table_from_file(
        io.BytesIO(ndjson_bytes), table_id, job_config=job_config
    )
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
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if args.load_only:
        load()
    else:
        rows = scrape(dry_run=args.dry_run)
        if not args.scrape_only and not args.dry_run:
            load()


if __name__ == "__main__":
    main()
"""
DraftKings PGA Tour Market Scraper.

Calls the Camoufox Cloud Run proxy to load DraftKings golf pages with real
browser TLS fingerprinting, captures internal API responses, parses
outcomeId / offer data for DraftKings deep links, and writes rows to
BigQuery table: sportsbook.raw_draftkings_pga_markets

Captures all available market types:
  - Tournament winner / outright
  - Round leader
  - Top 5 / Top 10 / Top 20 finishes
  - Make/miss cut
  - Head-to-head matchups
  - Stroke props (over/under)

Usage:
  python -m mobile_api.ingest.sportsbook.draftkings_pga_scraper --scrape-only
  python -m mobile_api.ingest.sportsbook.draftkings_pga_scraper --load-only
  python -m mobile_api.ingest.sportsbook.draftkings_pga_scraper --dry-run

Environment variables required:
  CAMOUFOX_SERVICE_URL  - Cloud Run URL
  CAMOUFOX_TOKEN        - GCP identity token
  GCP_PROJECT           - GCP project ID
  GOOGLE_APPLICATION_CREDENTIALS - path to service account JSON (load phase)

Fix notes (2026-03-28 v2):
  - Tightened capture_patterns from "api.draftkings.com" to specific path
    prefixes. The broad hostname was capturing:
      api.draftkings.com/eventstreams/v1/sdk/webeventtrackershim.js
      api.draftkings.com/eventstreams/v1/sdk/webeventtracker.js
      api.draftkings.com/geolocations/v1/locations.json
    None of these are odds responses. The actual offers come from:
      api.draftkings.com/lineups/v1/eventgroups/...
      api.draftkings.com/leagues/v1/...
  - Direct requests fallback restored (no proxy). api.draftkings.com does
    NOT enforce browser TLS fingerprinting — the geo check in the capture
    log returned successfully via plain requests. The proxy fallback was
    returning nothing because return_body is not supported by the proxy;
    a plain requests.get works fine here.
  - wait_ms increased to 25000 to capture all offer category XHRs.
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

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DATASET = "sportsbook"
TABLE = "raw_draftkings_pga_markets"
ARTIFACT_PATH = "/tmp/draftkings_pga_rows.ndjson"

# PGA Tour event group ID on DraftKings (stable across tournaments)
DK_PGA_EVENT_GROUP_ID = 88106

# Direct API — no browser TLS enforcement on api.draftkings.com
DK_OFFERS_URL = (
    "https://api.draftkings.com/lineups/v1/eventgroups/{event_group_id}"
    "?format=json"
)

SCRAPE_CONFIG = {
    "url": "https://sportsbook.draftkings.com/leagues/golf/pga-tour",
    "prime_url": "https://sportsbook.draftkings.com",
    # Tightened to path prefixes that only match odds/offers endpoints.
    # Excludes: eventstreams/ (analytics JS), geolocations/ (geo check)
    "capture_patterns": [
        "api.draftkings.com/lineups/",
        "api.draftkings.com/leagues/",
    ],
    "wait_ms": 25000,
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

def _decimal_from_american(american: int) -> Optional[float]:
    try:
        if american > 0:
            return round(american / 100 + 1, 4)
        return round(100 / (-american) + 1, 4)
    except Exception:
        return None


def _build_deep_link(outcome_id: str) -> str:
    return f"dksb://sb/addbet/{outcome_id}"


# ---------------------------------------------------------------------------
# Response filter
# ---------------------------------------------------------------------------

def _is_odds_response(body: Any) -> bool:
    """Accept only DK odds/offers payloads; reject everything else."""
    if not isinstance(body, dict):
        return False
    return bool(
        body.get("eventGroup")
        or body.get("offerCategories")
        or body.get("events")
        or body.get("leagues")
    )


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def _classify_market(name: str) -> str:
    n = name.lower()
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

    event_group = body.get("eventGroup") or body
    events_list = event_group.get("events", [])

    events_by_id: Dict[str, Dict[str, Any]] = {}
    for ev in events_list:
        if isinstance(ev, dict):
            eid = str(ev.get("eventId") or ev.get("id") or "")
            if eid:
                events_by_id[eid] = ev

    offer_categories = event_group.get("offerCategories", [])
    if not offer_categories:
        logger.debug("No offerCategories in DK response")
        return rows

    for cat in offer_categories:
        if not isinstance(cat, dict):
            continue
        cat_name = cat.get("name") or ""
        cat_id = str(cat.get("offerCategoryId") or "")

        for sub_desc in cat.get("offerSubcategoryDescriptors", []):
            if not isinstance(sub_desc, dict):
                continue
            sub_name = sub_desc.get("name") or ""
            sub_id = str(sub_desc.get("subcategoryId") or "")
            sub_cat = sub_desc.get("offerSubcategory", {})
            if not isinstance(sub_cat, dict):
                continue

            market_label = f"{cat_name} - {sub_name}".strip(" -")
            market_type = _classify_market(market_label)

            for offer_group in sub_cat.get("offers", []):
                if not isinstance(offer_group, list):
                    continue
                for offer in offer_group:
                    if not isinstance(offer, dict):
                        continue

                    offer_id = str(offer.get("offerId") or "")
                    event_id = str(offer.get("eventId") or "")
                    offer_label = offer.get("label") or ""

                    ev = events_by_id.get(event_id, {})
                    ev_name = ev.get("name") or ""
                    start_raw = ev.get("startDate") or ev.get("startDateTime") or ""
                    try:
                        event_start = (
                            datetime.fromisoformat(start_raw.replace("Z", "+00:00")).strftime("%Y-%m-%dT%H:%M:%S")
                            if start_raw else None
                        )
                    except Exception:
                        event_start = None

                    for outcome in offer.get("outcomes", []):
                        if not isinstance(outcome, dict):
                            continue

                        outcome_id = str(outcome.get("outcomeId") or outcome.get("id") or "")
                        outcome_label = outcome.get("label") or outcome.get("oddsType") or ""
                        line_val = outcome.get("line")
                        try:
                            outcome_line = float(line_val) if line_val is not None else None
                        except Exception:
                            outcome_line = None

                        odds_am_str = str(outcome.get("oddsAmerican") or outcome.get("odds") or "")
                        odds_dec: Optional[float] = None
                        try:
                            am_int = int(odds_am_str.replace("+", ""))
                            odds_dec = _decimal_from_american(am_int)
                        except Exception:
                            pass

                        rows.append({
                            "scraped_at": scraped_at,
                            "event_id": event_id or None,
                            "event_name": ev_name or offer_label or None,
                            "event_start": event_start,
                            "offer_id": offer_id or None,
                            "offer_label": offer_label or None,
                            "category_id": cat_id or None,
                            "category_name": cat_name or None,
                            "subcategory_id": sub_id or None,
                            "subcategory_name": sub_name or None,
                            "market_type": market_type,
                            "outcome_id": outcome_id or None,
                            "outcome_label": outcome_label or None,
                            "outcome_line": outcome_line,
                            "odds_american": odds_am_str or None,
                            "odds_decimal": odds_dec,
                            "deep_link": _build_deep_link(outcome_id) if outcome_id else None,
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
        if not _is_odds_response(body):
            logger.debug("  → skipping (not an odds response)")
            continue
        odds_hits += 1
        rows.extend(_parse_body(body, scraped_at=scraped_at))
    logger.info(
        "DraftKings PGA: %d captured, %d odds responses, %d rows parsed",
        len(captured), odds_hits, len(rows),
    )
    return rows


# ---------------------------------------------------------------------------
# Direct API fallback
# ---------------------------------------------------------------------------

def _fetch_direct(scraped_at: str) -> List[Dict[str, Any]]:
    """
    Hit api.draftkings.com/lineups/v1/eventgroups directly with plain requests.
    api.draftkings.com does NOT enforce browser TLS fingerprinting — confirmed
    by the geo check returning successfully via captured requests in prior runs.
    This is a reliable fallback when the browser capture misses the XHR window.
    """
    url = DK_OFFERS_URL.format(event_group_id=DK_PGA_EVENT_GROUP_ID)
    logger.info("DraftKings PGA: direct API fallback → %s", url)
    try:
        resp = requests.get(
            url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                "Accept": "application/json",
            },
            timeout=30,
        )
        resp.raise_for_status()
        body = resp.json()
        if not _is_odds_response(body):
            logger.warning("DraftKings PGA: direct API returned non-odds response")
            return []
        rows = _parse_body(body, scraped_at=scraped_at)
        logger.info("DraftKings PGA: direct API fallback returned %d rows", len(rows))
        return rows
    except Exception as exc:
        logger.warning("DraftKings PGA: direct API fallback failed: %s", exc)
        return []


# ---------------------------------------------------------------------------
# Scrape
# ---------------------------------------------------------------------------

def scrape(dry_run: bool = False) -> List[Dict[str, Any]]:
    cfg = SCRAPE_CONFIG
    logger.info("Calling Camoufox proxy for DraftKings PGA ...")
    result = _call_proxy({
        "url": cfg["url"],
        "prime_url": cfg["prime_url"],
        "capture_patterns": cfg["capture_patterns"],
        "wait_ms": cfg["wait_ms"],
        "timeout_ms": 90000,
    })

    captured = result.get("captured_requests", [])
    logger.info(
        "DraftKings PGA: page_status=%d  captured_requests=%d",
        result.get("status", 0),
        len(captured),
    )

    scraped_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows = _parse_captured(captured, scraped_at=scraped_at)

    # Direct fallback — api.draftkings.com doesn't require browser fingerprinting
    if not rows:
        logger.info("DraftKings PGA: 0 rows from browser capture — trying direct API fallback")
        rows = _fetch_direct(scraped_at=scraped_at)

    logger.info("Parsed %d total rows for DraftKings PGA", len(rows))

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
    parser = argparse.ArgumentParser(description="DraftKings PGA Tour market scraper")
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
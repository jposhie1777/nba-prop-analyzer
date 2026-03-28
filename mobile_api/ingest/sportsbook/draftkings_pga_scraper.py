"""
DraftKings PGA Tour Market Scraper.

Discovery findings (2026-03-28):
  - The generic /leagues/golf/pga-tour page loads a shell but never fires
    the odds XHR — markets are lazy-loaded on user interaction.
  - sportsbook-nash.draftkings.com appeared in discovery as the Nash API host.
  - sportsbook.draftkings.com/static/logos/provider/2/logos.json contains
    an 'Eventgroups' list that we can use to find the current tournament ID.

Strategy:
  1. Fetch the logos manifest to discover the current PGA event group ID.
  2. Hit the Nash API directly for that event group's markets.
  3. Fall back to the Camoufox browser capture on the specific tournament
     URL if the Nash API doesn't return odds data.

BigQuery table: sportsbook.raw_draftkings_pga_markets

Usage:
  python -m mobile_api.ingest.sportsbook.draftkings_pga_scraper --scrape-only
  python -m mobile_api.ingest.sportsbook.draftkings_pga_scraper --dry-run
  python -m mobile_api.ingest.sportsbook.draftkings_pga_scraper --load-only
  python -m mobile_api.ingest.sportsbook.draftkings_pga_scraper --discover
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
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

# Logos manifest — contains Eventgroups list with current tournament IDs.
# Discovered in network capture: sportsbook.draftkings.com/static/logos/provider/2/logos.json
DK_LOGOS_MANIFEST = "https://sportsbook.draftkings.com/static/logos/provider/2/logos.json"

# Nash API — the actual odds backend discovered in network capture.
# Format: /sites/{state}/api/leagues/v1/eventgroups/{id}/categories/{cat}/subcategories
DK_NASH_BASE = "https://sportsbook-nash.draftkings.com"
DK_NASH_OFFERS = (
    "{base}/sites/US-NJ-SB/api/leagues/v1/eventgroups/{event_group_id}"
    "/categories?format=json"
)

# Standard lineups API (backup)
DK_LINEUPS_URL = (
    "https://api.draftkings.com/lineups/v1/eventgroups/{event_group_id}"
    "?format=json"
)

# Golf provider ID on DraftKings (stable)
DK_GOLF_PROVIDER_ID = 2

SCRAPE_CONFIG = {
    # Navigate to the specific current Masters tournament page.
    # The generic /leagues/golf/pga-tour shell never fires the odds XHR.
    # Update this URL to the current tournament slug as needed.
    "url": "https://sportsbook.draftkings.com/leagues/golf/the-masters-88573",
    "prime_url": "https://sportsbook.draftkings.com",
    "capture_patterns": [
        "sportsbook-nash.draftkings.com/sites/",
        "api.draftkings.com/lineups/",
        "api.draftkings.com/leagues/",
    ],
    "wait_ms": 25000,
}

DISCOVER_PATTERNS = ["draftkings.com"]


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


_REQ_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}


# ---------------------------------------------------------------------------
# Event group discovery via logos manifest
# ---------------------------------------------------------------------------

def _discover_pga_event_group_id() -> Optional[int]:
    """
    Fetch the DK logos manifest and extract the current PGA Tour event group ID.
    The manifest lists all active event groups for each sport provider.
    Golf provider ID is 2.
    """
    try:
        resp = requests.get(DK_LOGOS_MANIFEST, headers=_REQ_HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        event_groups = data.get("Eventgroups", [])
        logger.info(
            "Logos manifest: found %d event groups for provider %d",
            len(event_groups), DK_GOLF_PROVIDER_ID,
        )
        for eg in event_groups:
            eg_id = eg.get("EventgroupId") or eg.get("eventGroupId") or eg.get("id")
            name = eg.get("Name") or eg.get("name") or ""
            logger.info("  EventgroupId=%s  Name=%s", eg_id, name)
            # Return the first one — there's usually only one active PGA event
            if eg_id:
                return int(eg_id)
    except Exception as exc:
        logger.warning("Could not fetch logos manifest: %s", exc)
    return None


# ---------------------------------------------------------------------------
# Nash API fetch
# ---------------------------------------------------------------------------

def _fetch_nash(event_group_id: int, scraped_at: str) -> List[Dict[str, Any]]:
    """
    Fetch odds from the Nash API directly.
    sportsbook-nash.draftkings.com is DK's internal odds API, discovered
    in the network capture log.
    """
    url = DK_NASH_OFFERS.format(
        base=DK_NASH_BASE,
        event_group_id=event_group_id,
    )
    logger.info("DraftKings PGA: Nash API → %s", url)
    try:
        resp = requests.get(url, headers=_REQ_HEADERS, timeout=30)
        resp.raise_for_status()
        body = resp.json()
        if _is_odds_response(body):
            rows = _parse_body(body, scraped_at=scraped_at)
            logger.info("DraftKings PGA: Nash API returned %d rows", len(rows))
            return rows
        logger.warning(
            "DraftKings PGA: Nash response doesn't look like odds data (keys: %s)",
            list(body.keys())[:8] if isinstance(body, dict) else type(body).__name__,
        )
    except Exception as exc:
        logger.warning("DraftKings PGA: Nash API failed: %s", exc)
    return []


def _fetch_lineups(event_group_id: int, scraped_at: str) -> List[Dict[str, Any]]:
    """Fallback to the standard lineups API."""
    url = DK_LINEUPS_URL.format(event_group_id=event_group_id)
    logger.info("DraftKings PGA: lineups API → %s", url)
    try:
        resp = requests.get(url, headers=_REQ_HEADERS, timeout=30)
        if resp.status_code == 404:
            logger.warning("DraftKings PGA: lineups API 404 for event group %d", event_group_id)
            return []
        resp.raise_for_status()
        body = resp.json()
        if _is_odds_response(body):
            rows = _parse_body(body, scraped_at=scraped_at)
            logger.info("DraftKings PGA: lineups API returned %d rows", len(rows))
            return rows
    except Exception as exc:
        logger.warning("DraftKings PGA: lineups API failed: %s", exc)
    return []


# ---------------------------------------------------------------------------
# Response filter
# ---------------------------------------------------------------------------

def _is_odds_response(body: Any) -> bool:
    if not isinstance(body, dict):
        return False
    return bool(
        body.get("eventGroup")
        or body.get("offerCategories")
        or body.get("events")
        or body.get("leagues")
        or body.get("categories")
        or body.get("eventGroupOffers")
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
        # Try Nash-specific structure
        offer_categories = body.get("categories") or body.get("eventGroupOffers") or []

    if not offer_categories:
        logger.debug("No offerCategories in DK response — raw keys: %s", list(body.keys())[:10])
        # Store raw for inspection
        rows.append({
            "scraped_at": scraped_at,
            "event_id": None,
            "event_name": None,
            "event_start": None,
            "offer_id": None,
            "offer_label": None,
            "category_id": None,
            "category_name": None,
            "subcategory_id": None,
            "subcategory_name": None,
            "market_type": "raw",
            "outcome_id": None,
            "outcome_label": None,
            "outcome_line": None,
            "odds_american": None,
            "odds_decimal": None,
            "deep_link": None,
            "raw_response": raw_str,
        })
        return rows

    for cat in offer_categories:
        if not isinstance(cat, dict):
            continue
        cat_name = cat.get("name") or ""
        cat_id = str(cat.get("offerCategoryId") or cat.get("id") or "")

        for sub_desc in cat.get("offerSubcategoryDescriptors", []):
            if not isinstance(sub_desc, dict):
                continue
            sub_name = sub_desc.get("name") or ""
            sub_id = str(sub_desc.get("subcategoryId") or sub_desc.get("id") or "")
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
                            if am_int > 0:
                                odds_dec = round(am_int / 100 + 1, 4)
                            else:
                                odds_dec = round(100 / (-am_int) + 1, 4)
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
                            "deep_link": f"dksb://sb/addbet/{outcome_id}" if outcome_id else None,
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
            continue
        odds_hits += 1
        rows.extend(_parse_body(body, scraped_at=scraped_at))
    logger.info(
        "DraftKings PGA: %d captured, %d odds responses, %d rows parsed",
        len(captured), odds_hits, len(rows),
    )
    return rows


# ---------------------------------------------------------------------------
# Discovery mode
# ---------------------------------------------------------------------------

def discover() -> None:
    logger.info("DISCOVERY MODE — capturing ALL draftkings.com requests")
    result = _call_proxy({
        "url": SCRAPE_CONFIG["url"],
        "prime_url": SCRAPE_CONFIG["prime_url"],
        "capture_patterns": DISCOVER_PATTERNS,
        "wait_ms": SCRAPE_CONFIG["wait_ms"],
        "timeout_ms": 90000,
    })

    captured = result.get("captured_requests", [])
    logger.info("page_status=%d  total_captured=%d", result.get("status", 0), len(captured))

    eg_pattern = re.compile(r"/eventgroups?/(\d+)")
    found_event_groups = set()

    print("\n=== DISCOVERED URLs ===")
    for i, capture in enumerate(captured):
        url = capture.get("url", "<unknown>")
        body = capture.get("body")
        top_keys = list(body.keys())[:6] if isinstance(body, dict) else "n/a"
        print(f"[{i:02d}] {url}")
        print(f"      body_type={type(body).__name__}  top_keys={top_keys}")
        m = eg_pattern.search(url)
        if m:
            found_event_groups.add(m.group(1))
    print("=== END ===\n")

    # Also try fetching event group from logos manifest
    logger.info("Fetching logos manifest to find current PGA event group...")
    eg_id = _discover_pga_event_group_id()
    if eg_id:
        found_event_groups.add(str(eg_id))

    if found_event_groups:
        print(f"EVENT GROUP IDs FOUND: {sorted(found_event_groups)}")
        print("ACTION: Update SCRAPE_CONFIG['url'] with the tournament slug and")
        print("        verify DK_LINEUPS_URL uses the correct event group ID.")
    else:
        print("No event group IDs found. Check logos manifest manually:")
        print(f"  {DK_LOGOS_MANIFEST}")


# ---------------------------------------------------------------------------
# Scrape
# ---------------------------------------------------------------------------

def scrape(dry_run: bool = False) -> List[Dict[str, Any]]:
    scraped_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Step 1: Discover current event group ID from logos manifest
    event_group_id = _discover_pga_event_group_id()
    if event_group_id:
        logger.info("DraftKings PGA: current event group ID = %d", event_group_id)
    else:
        logger.warning("DraftKings PGA: could not discover event group ID from manifest")

    # Step 2: Try Nash API directly (discovered in network capture)
    rows: List[Dict[str, Any]] = []
    if event_group_id:
        rows = _fetch_nash(event_group_id, scraped_at=scraped_at)

    # Step 3: Try lineups API
    if not rows and event_group_id:
        rows = _fetch_lineups(event_group_id, scraped_at=scraped_at)

    # Step 4: Browser capture on specific tournament page
    if not rows:
        logger.info("DraftKings PGA: API calls returned 0 rows — trying browser capture")
        cfg = SCRAPE_CONFIG
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
            result.get("status", 0), len(captured),
        )
        rows = _parse_captured(captured, scraped_at=scraped_at)

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
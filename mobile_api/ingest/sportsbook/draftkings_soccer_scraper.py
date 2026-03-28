"""
DraftKings Soccer Market Scraper.

Calls the Camoufox Cloud Run proxy to load DraftKings soccer pages with real
browser TLS fingerprinting, captures internal API responses, parses
outcomeId / offer data for DraftKings deep links, and writes rows to
BigQuery table: oddspedia.raw_draftkings_soccer_markets

Usage (called by sportsbook_soccer_markets.yml workflow):

  # Scrape only — write NDJSON to /tmp/draftkings_<league>_rows.ndjson
  python -m mobile_api.ingest.sportsbook.draftkings_soccer_scraper \
      --scrape-only --league EPL

  # Load only — read from /tmp/ and insert into BigQuery
  python -m mobile_api.ingest.sportsbook.draftkings_soccer_scraper \
      --load-only --league EPL

Environment variables required:
  CAMOUFOX_SERVICE_URL  - Cloud Run URL, e.g. https://camoufox-proxy-xxx.run.app
  CAMOUFOX_TOKEN        - Bearer token (GCP identity token for the service)
  GCP_PROJECT           - GCP project ID
  GOOGLE_APPLICATION_CREDENTIALS - path to service account JSON (load phase)
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
from typing import Any, Dict, List, Optional

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DATASET = "oddspedia"
TABLE = "raw_draftkings_soccer_markets"

# DraftKings soccer league pages and API domains to intercept.
# Target the odds API endpoint specifically — "api.draftkings.com" alone is
# too broad and captures JS CDN files (event-tracking SDK, etc.).
LEAGUE_CONFIG: Dict[str, Dict[str, Any]] = {
    "EPL": {
        "url": "https://sportsbook.draftkings.com/leagues/soccer/soccer-premier-league-88808",
        "prime_url": "https://sportsbook.draftkings.com",
        "capture_patterns": [
            "sportsbook.draftkings.com/api/odds",
            "api.draftkings.com/lineups/v1",
            "api.draftkings.com/offers/v",
        ],
        "wait_ms": 12000,
    },
    "MLS": {
        "url": "https://sportsbook.draftkings.com/leagues/soccer/soccer-major-league-soccer-88670",
        "prime_url": "https://sportsbook.draftkings.com",
        "capture_patterns": [
            "sportsbook.draftkings.com/api/odds",
            "api.draftkings.com/lineups/v1",
            "api.draftkings.com/offers/v",
        ],
        "wait_ms": 12000,
    },
}

ARTIFACT_PATTERN = "/tmp/draftkings_{league}_rows.ndjson"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_camoufox_url() -> str:
    url = os.environ.get("CAMOUFOX_SERVICE_URL", "").rstrip("/")
    if not url:
        raise RuntimeError("CAMOUFOX_SERVICE_URL env var is not set")
    return url


def _get_camoufox_token() -> str:
    return os.environ.get("CAMOUFOX_TOKEN", "")


def _call_proxy(payload: Dict[str, Any], max_retries: int = 3) -> Dict[str, Any]:
    """POST to the Camoufox proxy service with exponential-backoff retry.

    Retries on 500/503 which can occur when concurrent scrape jobs hit the
    single-concurrency Cloud Run instance simultaneously.
    """
    service_url = _get_camoufox_url()
    token = _get_camoufox_token()
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    delay = 15  # seconds between retries
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.post(
                f"{service_url}/fetch",
                json=payload,
                headers=headers,
                timeout=180,
            )
            if resp.status_code in (500, 503) and attempt < max_retries:
                logger.warning(
                    "proxy returned %d (attempt %d/%d), retrying in %ds ...",
                    resp.status_code, attempt, max_retries, delay,
                )
                time.sleep(delay)
                delay *= 2
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.Timeout:
            if attempt < max_retries:
                logger.warning("proxy timeout (attempt %d/%d), retrying in %ds ...", attempt, max_retries, delay)
                time.sleep(delay)
                delay *= 2
            else:
                raise
    raise RuntimeError("proxy call failed after all retries")


def _decimal_from_american(american: Optional[int]) -> Optional[float]:
    """Convert American odds integer to decimal odds."""
    if american is None:
        return None
    try:
        am = int(american)
        if am > 0:
            return round(am / 100 + 1, 4)
        return round(100 / (-am) + 1, 4)
    except Exception:
        return None


def _build_dk_deep_link(outcome_id: str) -> str:
    """
    Build a DraftKings single-outcome deep link.
    Multi-leg parlays use comma-separated outcome IDs:
      dksb://sb/addbet/OUTCOME1,OUTCOME2
    """
    return f"dksb://sb/addbet/{outcome_id}"


# ---------------------------------------------------------------------------
# Parsing — DraftKings API response formats
# ---------------------------------------------------------------------------

def _parse_dk_response(
    captured: List[Dict[str, Any]],
    league: str,
    scraped_at: str,
) -> List[Dict[str, Any]]:
    """
    Walk captured XHR responses and extract event + offer + outcome rows.

    DraftKings API v1/v2 offer responses look like:
      {
        "eventGroup": {
          "events": [...],
          "offerCategories": [
            {
              "offerSubcategoryDescriptors": [
                {
                  "offerSubcategory": {
                    "offers": [
                      [  // one inner list per event
                        {
                          "offerId": "...",
                          "label": "...",
                          "outcomes": [
                            {"outcomeId": "...", "label": "...", "oddsAmerican": "...", "line": 0}
                          ]
                        }
                      ]
                    ]
                  }
                }
              ]
            }
          ]
        }
      }
    """
    rows: List[Dict[str, Any]] = []

    for capture in captured:
        body = capture.get("body")
        raw_str = json.dumps(body) if isinstance(body, (dict, list)) else str(body)

        if not isinstance(body, dict):
            rows.append({"scraped_at": scraped_at, "league": league, "raw_response": raw_str[:64000]})
            continue

        # Build an event lookup from the eventGroup.events list
        event_group = body.get("eventGroup", {})
        if not event_group:
            # Some endpoints return offers at the top level
            event_group = body

        events_list = event_group.get("events", [])
        events_by_id: Dict[str, Dict[str, Any]] = {}
        for ev in events_list:
            if isinstance(ev, dict):
                eid = str(ev.get("eventId") or ev.get("id") or "")
                if eid:
                    events_by_id[eid] = ev

        offer_categories = event_group.get("offerCategories", [])
        if not offer_categories:
            # Fall back: store raw
            rows.append({"scraped_at": scraped_at, "league": league, "raw_response": raw_str[:64000]})
            continue

        for cat in offer_categories:
            if not isinstance(cat, dict):
                continue
            cat_id = str(cat.get("offerCategoryId") or "")
            for sub_desc in cat.get("offerSubcategoryDescriptors", []):
                if not isinstance(sub_desc, dict):
                    continue
                sub_id = str(sub_desc.get("subcategoryId") or "")
                sub_cat = sub_desc.get("offerSubcategory", {})
                if not isinstance(sub_cat, dict):
                    continue

                # offers is a list-of-lists: one inner list per event
                for offer_group in sub_cat.get("offers", []):
                    if not isinstance(offer_group, list):
                        continue
                    for offer in offer_group:
                        if not isinstance(offer, dict):
                            continue

                        offer_id = str(offer.get("offerId") or "")
                        event_id = str(offer.get("eventId") or "")
                        offer_label = offer.get("label") or offer.get("providerOfferId") or ""

                        ev = events_by_id.get(event_id, {})
                        home_team = ev.get("homeTeam", {}).get("teamName") or ev.get("name", "").split(" vs ")[0] if " vs " in (ev.get("name") or "") else ev.get("teamName1") or ""
                        away_team = ev.get("awayTeam", {}).get("teamName") or ev.get("name", "").split(" vs ")[-1] if " vs " in (ev.get("name") or "") else ev.get("teamName2") or ""
                        start_raw = ev.get("startDate") or ev.get("startDateTime") or ""
                        try:
                            event_start = (
                                datetime.fromisoformat(
                                    start_raw.replace("Z", "+00:00")
                                ).strftime("%Y-%m-%dT%H:%M:%S")
                                if start_raw
                                else None
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

                            deep_link = _build_dk_deep_link(outcome_id) if outcome_id else None

                            rows.append(
                                {
                                    "scraped_at": scraped_at,
                                    "league": league,
                                    "event_id": event_id or None,
                                    "home_team": home_team or None,
                                    "away_team": away_team or None,
                                    "event_start": event_start,
                                    "offer_id": offer_id or None,
                                    "category_id": cat_id or None,
                                    "subcategory_id": sub_id or None,
                                    "outcome_id": outcome_id or None,
                                    "outcome_label": outcome_label or None,
                                    "outcome_line": outcome_line,
                                    "odds_american": odds_am_str or None,
                                    "odds_decimal": odds_dec,
                                    "deep_link": deep_link,
                                    "raw_response": raw_str[:64000],
                                }
                            )

    return rows


# ---------------------------------------------------------------------------
# Scrape phase
# ---------------------------------------------------------------------------

def scrape(league: str, dry_run: bool = False) -> List[Dict[str, Any]]:
    cfg = LEAGUE_CONFIG.get(league.upper())
    if not cfg:
        raise ValueError(f"Unknown league '{league}'. Choose from: {list(LEAGUE_CONFIG)}")

    logger.info("Calling Camoufox proxy for DraftKings %s ...", league)
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
        "DraftKings %s: page_status=%d  captured_requests=%d",
        league,
        result.get("status", 0),
        len(captured),
    )

    scraped_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows = _parse_dk_response(captured, league=league, scraped_at=scraped_at)

    logger.info("Parsed %d rows for DraftKings %s", len(rows), league)

    if dry_run:
        for row in rows[:5]:
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

    errors = client.insert_rows_json(table_id, rows)
    if errors:
        logger.error("BigQuery insert errors: %s", errors)
        sys.exit(1)
    logger.info("Inserted %d rows into %s", len(rows), table_id)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="DraftKings soccer market scraper")
    parser.add_argument("--league", default="EPL", help="EPL or MLS")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--scrape-only", action="store_true")
    group.add_argument("--load-only", action="store_true")
    parser.add_argument("--dry-run", action="store_true", help="Print rows, skip BQ write")
    args = parser.parse_args()

    if args.load_only:
        load(args.league)
    else:
        rows = scrape(args.league, dry_run=args.dry_run)
        if not args.scrape_only and not args.dry_run:
            load(args.league)


if __name__ == "__main__":
    main()

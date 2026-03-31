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

Fix notes (2026-03-28):
  - Removed "sportsbook.draftkings.com/api" from capture_patterns — it was
    too broad and matched JS bundle responses (analytics SDK, etc.) instead
    of the actual odds API. Only "api.draftkings.com" is used now.
  - Increased wait_ms from 8000 → 12000 to give the odds API calls time to
    fire after initial page render.
  - Added direct API fallback: if 0 useful captured requests come back, the
    scraper hits the DraftKings eventgroup API directly (no browser needed)
    and parses that response instead. This covers cases where the browser
    capture misses the XHR window.
  - Tightened _is_odds_response() guard so analytics/JS blob captures are
    silently skipped rather than stored as raw_response rows.
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

# DK eventgroup IDs for the direct API fallback.
# Find these by inspecting network tab on the league page:
#   https://sportsbook.draftkings.com/leagues/soccer/soccer-major-league-soccer-88670
#   XHR to: api.draftkings.com/leagues/v1/categories/<eventGroupId>/...
DK_EVENTGROUP_IDS: Dict[str, int] = {
    "EPL": 88808,
    "MLS": 88670,
}

# DK direct API — no auth required, public endpoint.
# Returns the same payload the browser XHR gets.
DK_API_URL = (
    "https://api.draftkings.com/lineups/v1/teasers"  # not used directly
)
DK_EVENTGROUP_URL = (
    "https://api.draftkings.com/leagues/v1/categories/{event_group_id}"
    "/categoriesdisplay?format=json"
)
DK_OFFERS_URL = (
    "https://api.draftkings.com/lineups/v1/eventgroups/{event_group_id}"
    "?format=json"
)

LEAGUE_CONFIG: Dict[str, Dict[str, Any]] = {
    "EPL": {
        "url": "https://sportsbook.draftkings.com/leagues/soccer/soccer-premier-league-88808",
        "prime_url": "https://sportsbook.draftkings.com",
        # Only capture from the actual odds API domain.
        # "sportsbook.draftkings.com/api" removed — too broad, matches JS bundles.
        "capture_patterns": [
            "api.draftkings.com",
        ],
        "wait_ms": 12000,
    },
    "MLS": {
        "url": "https://sportsbook.draftkings.com/leagues/soccer/soccer-major-league-soccer-88670",
        "prime_url": "https://sportsbook.draftkings.com",
        "capture_patterns": [
            "api.draftkings.com",
        ],
        "wait_ms": 12000,
    },
}

ARTIFACT_PATTERN = "/tmp/draftkings_{league}_rows.ndjson"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

from mobile_api.ingest.sportsbook.camoufox_client import (
    call_proxy as _call_proxy,
    get_camoufox_url as _get_camoufox_url,
)


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


def _is_odds_response(body: Any) -> bool:
    """
    Return True only if this captured response looks like a DK odds/offers
    payload. Rejects analytics JS blobs and other non-JSON captures.
    """
    if not isinstance(body, dict):
        return False
    # Must have at least one of these top-level keys that DK odds responses use
    return bool(
        body.get("eventGroup")
        or body.get("offerCategories")
        or body.get("events")
        or body.get("leagues")
    )


# ---------------------------------------------------------------------------
# Direct API fallback
# ---------------------------------------------------------------------------

def _fetch_direct(league: str) -> Optional[Dict[str, Any]]:
    """
    Hit the DraftKings public eventgroup API directly (no browser needed).
    Used as a fallback when the Camoufox proxy captures 0 odds responses.
    Returns the parsed JSON body or None on failure.
    """
    event_group_id = DK_EVENTGROUP_IDS.get(league.upper())
    if not event_group_id:
        return None

    url = DK_OFFERS_URL.format(event_group_id=event_group_id)
    logger.info("DraftKings %s: falling back to direct API call → %s", league, url)
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
        return resp.json()
    except Exception as exc:
        logger.warning("Direct API fallback failed for DraftKings %s: %s", league, exc)
        return None


# ---------------------------------------------------------------------------
# Parsing — DraftKings API response formats
# ---------------------------------------------------------------------------

def _parse_dk_body(
    body: Dict[str, Any],
    league: str,
    scraped_at: str,
) -> List[Dict[str, Any]]:
    """
    Parse a single DK eventGroup JSON body into flat rows.

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
                    ev_name = ev.get("name") or ""
                    home_team = (
                        ev.get("homeTeam", {}).get("teamName")
                        or (ev_name.split(" vs ")[0] if " vs " in ev_name else "")
                        or ev.get("teamName1")
                        or ""
                    )
                    away_team = (
                        ev.get("awayTeam", {}).get("teamName")
                        or (ev_name.split(" vs ")[-1] if " vs " in ev_name else "")
                        or ev.get("teamName2")
                        or ""
                    )
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
                        outcome_id = str(
                            outcome.get("outcomeId") or outcome.get("id") or ""
                        )
                        outcome_label = (
                            outcome.get("label") or outcome.get("oddsType") or ""
                        )
                        line_val = outcome.get("line")
                        try:
                            outcome_line = float(line_val) if line_val is not None else None
                        except Exception:
                            outcome_line = None

                        odds_am_str = str(
                            outcome.get("oddsAmerican") or outcome.get("odds") or ""
                        )
                        odds_dec: Optional[float] = None
                        try:
                            am_int = int(odds_am_str.replace("+", ""))
                            odds_dec = _decimal_from_american(am_int)
                        except Exception:
                            pass

                        deep_link = (
                            _build_dk_deep_link(outcome_id) if outcome_id else None
                        )

                        rows.append(
                            {
                                "scraped_at": scraped_at,
                                "league": league,
                                "event_id": event_id or None,
                                "home_team": home_team or None,
                                "away_team": away_team or None,
                                "event_start": event_start,
                                "offer_id": offer_id or None,
                                "offer_label": offer_label or None,
                                "category_id": cat_id or None,
                                "subcategory_id": sub_id or None,
                                "outcome_id": outcome_id or None,
                                "outcome_label": outcome_label or None,
                                "outcome_line": outcome_line,
                                "odds_american": odds_am_str or None,
                                "odds_decimal": odds_dec,
                                "deep_link": deep_link,
                                "raw_response": raw_str,
                            }
                        )

    return rows


def _parse_dk_response(
    captured: List[Dict[str, Any]],
    league: str,
    scraped_at: str,
) -> List[Dict[str, Any]]:
    """Walk captured XHR responses, skipping non-odds payloads."""
    rows: List[Dict[str, Any]] = []
    odds_hits = 0

    for capture in captured:
        body = capture.get("body")
        if not _is_odds_response(body):
            # Skip analytics JS blobs, geo checks, etc.
            logger.debug("Skipping non-odds capture (keys: %s)", list(body.keys()) if isinstance(body, dict) else type(body).__name__)
            continue
        odds_hits += 1
        rows.extend(_parse_dk_body(body, league=league, scraped_at=scraped_at))

    logger.info(
        "DraftKings %s: %d captured requests, %d looked like odds responses",
        league, len(captured), odds_hits,
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

    # Fallback: if browser capture yielded no odds rows, try the direct API
    if not rows:
        logger.info(
            "DraftKings %s: 0 odds rows from browser capture — trying direct API fallback",
            league,
        )
        body = _fetch_direct(league)
        if body:
            rows = _parse_dk_body(body, league=league, scraped_at=scraped_at)
            logger.info(
                "DraftKings %s: direct API fallback returned %d rows", league, len(rows)
            )
        else:
            logger.warning(
                "DraftKings %s: direct API fallback also returned nothing", league
            )

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

    # Use load_table_from_file with NDJSON to avoid SSL context issues
    # with insert_rows_json in Cloud Run / Camoufox environments
    import io
    from google.cloud.bigquery import LoadJobConfig, SourceFormat

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
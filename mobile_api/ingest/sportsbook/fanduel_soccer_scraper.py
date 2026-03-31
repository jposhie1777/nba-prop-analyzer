"""
FanDuel Soccer Market Scraper.

Calls the Camoufox Cloud Run proxy to load FanDuel soccer pages with real
browser TLS fingerprinting, captures internal API responses, parses
marketId + selectionId values for FanDuel deep links, and writes rows to
BigQuery table: oddspedia.raw_fanduel_soccer_markets

Usage (called by sportsbook_soccer_markets.yml workflow):

  # Scrape only — write NDJSON to /tmp/fanduel_<league>_rows.ndjson
  python -m mobile_api.ingest.sportsbook.fanduel_soccer_scraper \
      --scrape-only --league EPL

  # Load only — read from /tmp/ and insert into BigQuery
  python -m mobile_api.ingest.sportsbook.fanduel_soccer_scraper \
      --load-only --league EPL

Environment variables required:
  CAMOUFOX_SERVICE_URL  - Cloud Run URL, e.g. https://camoufox-proxy-xxx.run.app
  CAMOUFOX_TOKEN        - Bearer token (GCP identity token for the service)
  GCP_PROJECT           - GCP project ID
  GOOGLE_APPLICATION_CREDENTIALS - path to service account JSON (load phase)

Fix notes (2026-03-28):
  - Increased wait_ms from 8000 → 15000. FanDuel's MLS page lazy-loads event
    data after initial render; the previous 8s window closed before the main
    events XHR fired, capturing only the empty facets/browse response.
  - Added _is_fanduel_odds_response() guard to skip the facets/browse response
    (which has competition metadata but results:[]) and other non-market blobs.
  - Added direct API fallback via FanDuel's internal tab/event API for cases
    where browser capture still misses the XHR window.
  - load() switched to load_table_from_file + NDJSON (avoids SSL context
    corruption seen with insert_rows_json in Camoufox environments).
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DATASET = "oddspedia"
TABLE = "raw_fanduel_soccer_markets"

# FanDuel tab IDs for the direct API fallback.
# Tab IDs are stable per competition; find them by inspecting:
#   GET https://sbapi.fanduel.com/api/content-managed-page?page=SPORT&...
#   or from the tab parameter in the page URL after navigating.
FD_TAB_IDS: Dict[str, str] = {
    "EPL": "61020",   # Premier League tab
    "MLS": "101",     # MLS tab
}

# FanDuel internal event API (no auth required, same payload browser gets)
FD_EVENTS_URL = (
    "https://sbapi.fanduel.com/api/content-managed-page"
    "?page=SPORT&eventTypeId=1"
    "&_ak=FhMFpcPWXMeyZxOx"
    "&competitionId={competition_id}"
    "&tab=featured"
)
FD_COMPETITION_IDS: Dict[str, str] = {
    "EPL": "10932509",   # Premier League competition ID
    "MLS": "141",        # MLS competition ID
}

LEAGUE_CONFIG: Dict[str, Dict[str, Any]] = {
    "EPL": {
        "url": "https://sportsbook.fanduel.com/soccer/premier-league",
        "prime_url": "https://sportsbook.fanduel.com",
        # Tightened: only capture from known FanDuel API domains.
        # "sportsbook.fanduel.com/api" removed — not a real endpoint pattern,
        # FD uses sbapi.fanduel.com and sportsbook-nash.fanduel.com for XHRs.
        "capture_patterns": [
            "sbapi.fanduel.com",
            "sportsbook-nash.fanduel.com",
            "api.fanduel.com",
        ],
        # Increased from 8000 → 15000: FD lazy-loads events after initial render
        "wait_ms": 15000,
    },
    "MLS": {
        "url": "https://sportsbook.fanduel.com/soccer/mls",
        "prime_url": "https://sportsbook.fanduel.com",
        "capture_patterns": [
            "sbapi.fanduel.com",
            "sportsbook-nash.fanduel.com",
            "api.fanduel.com",
        ],
        "wait_ms": 15000,
    },
}

ARTIFACT_PATTERN = "/tmp/fanduel_{league}_rows.ndjson"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

from mobile_api.ingest.sportsbook.camoufox_client import (
    call_proxy as _call_proxy,
    get_camoufox_url as _get_camoufox_url,
)


def _american_from_decimal(decimal: float) -> str:
    """Convert decimal odds to American format string."""
    if decimal <= 1.0:
        return "N/A"
    if decimal >= 2.0:
        return f"+{int(round((decimal - 1) * 100))}"
    return str(int(round(-100 / (decimal - 1))))


def _is_fanduel_odds_response(body: Any) -> bool:
    """
    Return True only if this captured response looks like a FanDuel events/
    markets payload with actual event data — not the empty facets/browse
    response or other metadata blobs.

    The facets response looks like:
      {"facets": [...], "results": [], "attachments": {...}}
    A real events response has non-empty "results" or an "events" list.
    """
    if not isinstance(body, dict):
        return False

    # Reject the empty browse/facets response
    if "facets" in body and not body.get("results"):
        return False

    # Accept if there are events in any of the known response shapes
    events = (
        body.get("events")
        or body.get("data", {}).get("events")
        or body.get("result", {}).get("events")
        or (body.get("attachments") or {}).get("events")
        or (body.get("results") or [])
    )
    if isinstance(events, dict):
        return len(events) > 0
    if isinstance(events, list):
        return len(events) > 0
    return False


# ---------------------------------------------------------------------------
# Direct API fallback
# ---------------------------------------------------------------------------

def _fetch_direct(league: str) -> Optional[Dict[str, Any]]:
    """
    Hit the FanDuel sbapi directly for event/market data.
    Used as a fallback when the Camoufox proxy captures 0 useful responses.
    Returns the parsed JSON body or None on failure.
    """
    competition_id = FD_COMPETITION_IDS.get(league.upper())
    if not competition_id:
        return None

    url = FD_EVENTS_URL.format(competition_id=competition_id)
    logger.info("FanDuel %s: falling back to direct API call → %s", league, url)
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
                "Origin": "https://sportsbook.fanduel.com",
                "Referer": "https://sportsbook.fanduel.com/",
            },
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        logger.warning("Direct API fallback failed for FanDuel %s: %s", league, exc)
        return None


# ---------------------------------------------------------------------------
# Parsing — FanDuel API response formats
# ---------------------------------------------------------------------------

def _parse_fanduel_body(
    body: Dict[str, Any],
    league: str,
    scraped_at: str,
) -> List[Dict[str, Any]]:
    """
    Parse a single FanDuel events/markets JSON body into flat rows.

    FanDuel's internal API responses vary by tech stack version (SBTech vs
    Nash). We try multiple parsing strategies.
    """
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
        # SBTech payload: attachments.events is a dict keyed by eventId
        attachments_events = (body.get("attachments") or {}).get("events", {})
        if not events and isinstance(attachments_events, dict):
            events = list(attachments_events.values())
        # results list (Nash)
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
            event.get("eventId")
            or event.get("id")
            or event.get("event_id")
            or ""
        )
        home_team = (
            event.get("homeTeamName")
            or event.get("home", {}).get("name")
            or (event.get("participants") or [{}])[0].get("name")
            or ""
        )
        away_team = (
            event.get("awayTeamName")
            or event.get("away", {}).get("name")
            or ((event.get("participants") or [{}, {}])[1:2] or [{}])[0].get("name")
            or ""
        )
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

                name_lower = selection_name.lower()
                if "draw" in name_lower or "tie" in name_lower:
                    side = "draw"
                elif home_team and home_team.lower() in name_lower:
                    side = "home"
                elif away_team and away_team.lower() in name_lower:
                    side = "away"
                elif runner.get("side"):
                    side = runner["side"].lower()
                else:
                    side = None

                deep_link = None
                if market_id and selection_id:
                    params = urlencode(
                        [("marketId[]", market_id), ("selectionId[]", selection_id)]
                    )
                    deep_link = (
                        f"fanduelsportsbook://launch?deepLink=addToBetslip%3F{params}"
                    )

                rows.append(
                    {
                        "scraped_at": scraped_at,
                        "league": league,
                        "event_id": event_id or None,
                        "home_team": home_team or None,
                        "away_team": away_team or None,
                        "event_start": event_start,
                        "market_id": market_id or None,
                        "market_name": market_name or None,
                        "selection_id": selection_id or None,
                        "selection_name": selection_name or None,
                        "outcome_side": side,
                        "handicap": handicap_f,
                        "odds_decimal": odds_dec,
                        "odds_american": odds_am,
                        "deep_link": deep_link,
                        "raw_response": raw_str,
                    }
                )

    return rows


def _parse_fanduel_response(
    captured: List[Dict[str, Any]],
    league: str,
    scraped_at: str,
) -> List[Dict[str, Any]]:
    """Walk captured XHR responses, skipping non-events payloads."""
    rows: List[Dict[str, Any]] = []
    odds_hits = 0

    for capture in captured:
        body = capture.get("body")
        if not _is_fanduel_odds_response(body):
            logger.debug(
                "Skipping non-events FanDuel capture (keys: %s)",
                list(body.keys()) if isinstance(body, dict) else type(body).__name__,
            )
            continue
        odds_hits += 1
        rows.extend(_parse_fanduel_body(body, league=league, scraped_at=scraped_at))

    logger.info(
        "FanDuel %s: %d captured requests, %d looked like events responses",
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

    logger.info("Calling Camoufox proxy for FanDuel %s ...", league)
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
        "FanDuel %s: page_status=%d  captured_requests=%d",
        league,
        result.get("status", 0),
        len(captured),
    )

    scraped_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows = _parse_fanduel_response(captured, league=league, scraped_at=scraped_at)

    # Fallback: if browser capture yielded no market rows, try the direct API
    if not rows:
        logger.info(
            "FanDuel %s: 0 market rows from browser capture — trying direct API fallback",
            league,
        )
        body = _fetch_direct(league)
        if body and _is_fanduel_odds_response(body):
            rows = _parse_fanduel_body(body, league=league, scraped_at=scraped_at)
            logger.info(
                "FanDuel %s: direct API fallback returned %d rows", league, len(rows)
            )
        else:
            logger.warning(
                "FanDuel %s: direct API fallback also returned nothing "
                "(likely no fixtures scheduled today)",
                league,
            )

    logger.info("Parsed %d rows for FanDuel %s", len(rows), league)

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

    # Use load_table_from_file with NDJSON to avoid SSL context issues
    # with insert_rows_json in Cloud Run / Camoufox environments
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
    parser = argparse.ArgumentParser(description="FanDuel soccer market scraper")
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
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

# FanDuel soccer league URLs and the API domains to intercept.
# From live captures, FanDuel's event/market data comes from:
#   sportsbook.fanduel.com/api/content-managed-page (events + markets JSON)
#   sbapi.sbtech.com  (legacy SBTech odds API, still used for some markets)
#   sportsbook-nash.fanduel.com (newer Nash API, GraphQL/REST)
# We wait 15s to ensure all deferred API calls have fired after hydration.
LEAGUE_CONFIG: Dict[str, Dict[str, Any]] = {
    "EPL": {
        "url": "https://sportsbook.fanduel.com/soccer/premier-league",
        "prime_url": "https://sportsbook.fanduel.com",
        "capture_patterns": [
            "sportsbook.fanduel.com/api/content-managed-page",
            "sportsbook.fanduel.com/api/",
            "sbapi.sbtech.com",
            "sportsbook-nash.fanduel.com",
            "sbapi.fanduel.com",
        ],
        "wait_ms": 15000,
    },
    "MLS": {
        "url": "https://sportsbook.fanduel.com/soccer/mls",
        "prime_url": "https://sportsbook.fanduel.com",
        "capture_patterns": [
            "sportsbook.fanduel.com/api/content-managed-page",
            "sportsbook.fanduel.com/api/",
            "sbapi.sbtech.com",
            "sportsbook-nash.fanduel.com",
            "sbapi.fanduel.com",
        ],
        "wait_ms": 15000,
    },
}

ARTIFACT_PATTERN = "/tmp/fanduel_{league}_rows.ndjson"


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


def _american_from_decimal(decimal: float) -> str:
    """Convert decimal odds to American format string."""
    if decimal <= 1.0:
        return "N/A"
    if decimal >= 2.0:
        return f"+{int(round((decimal - 1) * 100))}"
    return str(int(round(-100 / (decimal - 1))))


# ---------------------------------------------------------------------------
# Parsing  — FanDuel API response formats
# ---------------------------------------------------------------------------

def _parse_fanduel_response(
    captured: List[Dict[str, Any]],
    league: str,
    scraped_at: str,
) -> List[Dict[str, Any]]:
    """
    Walk captured XHR responses and extract event + market + selection rows.

    FanDuel's internal API responses vary by tech stack version (SBTech vs
    Nash). We try multiple parsing strategies and fall back to storing the
    raw JSON so it can be re-processed later.
    """
    rows: List[Dict[str, Any]] = []

    for capture in captured:
        body = capture.get("body")
        raw_str = json.dumps(body) if isinstance(body, (dict, list)) else str(body)

        if isinstance(body, list):
            events = body
        elif isinstance(body, dict):
            # Try common top-level keys
            events = (
                body.get("events")
                or body.get("data", {}).get("events")
                or body.get("result", {}).get("events")
                or body.get("attachments", {}).get("events", {})
                or []
            )
            # SBTech payload structure: {"data": [...]}
            if not events and isinstance(body.get("data"), list):
                events = body["data"]
        else:
            continue

        if not events:
            # Store the raw capture as a single unparsed row for debugging
            rows.append(
                {
                    "scraped_at": scraped_at,
                    "league": league,
                    "raw_response": raw_str[:64000],
                }
            )
            continue

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
                or event.get("participants", [{}])[0].get("name")
                or ""
            )
            away_team = (
                event.get("awayTeamName")
                or event.get("away", {}).get("name")
                or (event.get("participants", [{}, {}])[1:2] or [{}])[0].get("name")
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

            # Markets / runners
            markets = (
                event.get("markets")
                or event.get("marketGroups")
                or []
            )
            for market in markets:
                if not isinstance(market, dict):
                    continue
                market_id = str(
                    market.get("marketId")
                    or market.get("id")
                    or ""
                )
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
                        runner.get("selectionId")
                        or runner.get("id")
                        or ""
                    )
                    selection_name = runner.get("runnerName") or runner.get("name") or ""
                    handicap = runner.get("handicap") or runner.get("line")
                    try:
                        handicap_f = float(handicap) if handicap is not None else None
                    except Exception:
                        handicap_f = None

                    # Decimal odds
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

                    # Outcome side heuristic
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

                    # FanDuel deep link
                    deep_link = None
                    if market_id and selection_id:
                        params = urlencode(
                            [("marketId[]", market_id), ("selectionId[]", selection_id)]
                        )
                        deep_link = f"fanduelsportsbook://launch?deepLink=addToBetslip%3F{params}"

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

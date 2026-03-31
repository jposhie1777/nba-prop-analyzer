"""
FanDuel ATP Tennis Market Scraper.

Uses the Camoufox Cloud Run proxy to load the FanDuel tennis sport page and
capture internal XHR responses (content-managed-page), then fetches the full
market suite per ATP event via event-page — trying direct API first, falling
back to Camoufox if that fails.

ATP-only: events are included only when their competition name starts with
"ATP" (e.g. "ATP Houston TX 2026", "ATP Marrakech 2026"). WTA, ITF,
Challenger, and UTR events are all excluded.

Markets captured per match:
  MATCH_BETTING, TO_WIN_1ST_SET, SET_2_WINNER, SET_BETTING,
  CORRECT_SCORE_1ST_SET, SET_X_SCORE_AFTER_Y_GAMES,
  OVER_UNDER_GAME_HANDICAP, TOTAL_MATCH_GAMES, GAME_HANDICAP_3WAY,
  SET_X_GAME_HANDICAP, SET_X_GAME_HANDICAP_3_WAY,
  PLAYER_A/B_SCORE_OF_FIRST_SERVICE_GAME, ALTERNATIVE_TOTAL_GAMES,
  ALTERNATIVE_GAME_HANDICAP, ALTERNATIVE_SET_HANDICAP, and more.

Output BigQuery table: sportsbook.raw_fanduel_atp_markets

Usage:
  python -m mobile_api.ingest.sportsbook.fanduel_atp_scraper --scrape-only
  python -m mobile_api.ingest.sportsbook.fanduel_atp_scraper --load-only
  python -m mobile_api.ingest.sportsbook.fanduel_atp_scraper --dry-run

Environment variables:
  CAMOUFOX_SERVICE_URL             - Cloud Run proxy URL
  CAMOUFOX_TOKEN                   - GCP identity token for the proxy
  GCP_PROJECT                      - GCP project ID
  GOOGLE_APPLICATION_CREDENTIALS   - path to service account JSON (load phase)
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

DATASET = "sportsbook"
TABLE = "raw_fanduel_atp_markets"
ARTIFACT_PATH = "/tmp/fanduel_atp_rows.ndjson"

AK = "FhMFpcPWXMeyZxOx"

TENNIS_SPORT_URL = "https://sportsbook.fanduel.com/tennis"

SPORT_PAGE_API_URL = (
    "https://api.sportsbook.fanduel.com/sbapi/content-managed-page"
    f"?page=SPORT&eventTypeId=2&_ak={AK}&timezone=America%2FNew_York"
)

EVENT_PAGE_API_URL = (
    "https://api.sportsbook.fanduel.com/sbapi/event-page"
    f"?_ak={AK}"
    "&eventId={event_id}"
    "&useQuickBets=true"
    "&useCombinedTouchdownsVirtualMarket=true"
)

# FanDuel event page URL for Camoufox browser loads
EVENT_BROWSER_URL = "https://sportsbook.fanduel.com/tennis/event/{event_id}"

# Capture patterns for Camoufox
CAPTURE_PATTERNS = [
    "api.sportsbook.fanduel.com",
    "sbapi.fanduel.com",
]

ATP_PREFIX = "ATP"

# Polite delay between event API calls
EVENT_PAGE_DELAY_S = 0.3

DIRECT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Origin": "https://sportsbook.fanduel.com",
    "Referer": "https://sportsbook.fanduel.com/tennis",
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
    """POST to the Camoufox proxy and return parsed JSON response."""
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
# Response validators
# ---------------------------------------------------------------------------

def _is_content_managed_page(body: Any) -> bool:
    """Return True if this body looks like a content-managed-page response."""
    if not isinstance(body, dict):
        return False
    attachments = body.get("attachments", {})
    return bool(attachments.get("events")) and bool(attachments.get("competitions"))


def _is_event_page(body: Any) -> bool:
    """Return True if this body looks like an event-page response with markets."""
    if not isinstance(body, dict):
        return False
    attachments = body.get("attachments", {})
    return bool(attachments.get("markets"))


# ---------------------------------------------------------------------------
# Step 1 — Enumerate ATP events
# ---------------------------------------------------------------------------

def _parse_atp_events(body: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract ATP-only events from a content-managed-page response body."""
    attachments = body.get("attachments", {})
    competitions: Dict[str, Any] = attachments.get("competitions", {})
    events_map: Dict[str, Any] = attachments.get("events", {})

    atp_comp_ids = {
        str(comp_id)
        for comp_id, comp in competitions.items()
        if comp.get("name", "").startswith(ATP_PREFIX)
    }
    logger.info(
        "Found %d ATP competitions: %s",
        len(atp_comp_ids),
        [competitions[c]["name"] for c in atp_comp_ids if c in competitions],
    )

    atp_events = []
    for event_id, event in events_map.items():
        comp_id = str(event.get("competitionId", ""))
        if comp_id not in atp_comp_ids:
            continue
        comp = competitions.get(comp_id, {})
        atp_events.append(
            {
                "event_id": str(event_id),
                "event_name": event.get("name", ""),
                "competition_id": comp_id,
                "tournament_name": comp.get("name", ""),
                "open_date": event.get("openDate", ""),
                "in_play": event.get("inPlay", False),
            }
        )

    logger.info("Found %d ATP events", len(atp_events))
    return atp_events


def fetch_atp_events_via_camoufox() -> List[Dict[str, Any]]:
    """
    Load the FanDuel tennis page via Camoufox and parse ATP events from the
    captured content-managed-page XHR response.
    Falls back to a direct API call if Camoufox captures nothing useful.
    """
    logger.info("Camoufox: loading tennis sport page → %s", TENNIS_SPORT_URL)
    result = _call_proxy(
        {
            "url": TENNIS_SPORT_URL,
            "prime_url": "https://sportsbook.fanduel.com",
            "capture_patterns": CAPTURE_PATTERNS,
            "wait_ms": 15000,
            "timeout_ms": 60000,
        }
    )

    captured = result.get("captured_requests", [])
    logger.info(
        "Tennis sport page: status=%d  captured=%d",
        result.get("status", 0),
        len(captured),
    )

    for capture in captured:
        body = capture.get("body")
        if _is_content_managed_page(body):
            logger.info("Found content-managed-page in captured XHRs")
            return _parse_atp_events(body)

    # Direct API fallback
    logger.info("No content-managed-page captured — trying direct API fallback")
    try:
        resp = requests.get(SPORT_PAGE_API_URL, headers=DIRECT_HEADERS, timeout=30)
        resp.raise_for_status()
        body = resp.json()
        if _is_content_managed_page(body):
            logger.info("Direct API fallback succeeded for sport page")
            return _parse_atp_events(body)
    except Exception as exc:
        logger.warning("Direct API fallback for sport page failed: %s", exc)

    logger.warning("Could not retrieve ATP events — returning empty list")
    return []


# ---------------------------------------------------------------------------
# Step 2 — Fetch full market suite per event
# ---------------------------------------------------------------------------

def fetch_event_page(event_meta: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Fetch all markets for a single ATP event.
    Tries direct API first; falls back to Camoufox if the response is empty
    or missing markets.
    """
    event_id = event_meta["event_id"]
    api_url = EVENT_PAGE_API_URL.format(event_id=event_id)

    # --- Try direct API first ---
    try:
        resp = requests.get(api_url, headers=DIRECT_HEADERS, timeout=30)
        resp.raise_for_status()
        body = resp.json()
        if _is_event_page(body):
            return body
        logger.info(
            "Direct API returned no markets for event %s — falling back to Camoufox",
            event_id,
        )
    except Exception as exc:
        logger.info(
            "Direct API failed for event %s (%s) — falling back to Camoufox",
            event_id,
            exc,
        )

    # --- Camoufox fallback ---
    browser_url = EVENT_BROWSER_URL.format(event_id=event_id)
    logger.info("Camoufox: loading event page → %s", browser_url)
    try:
        result = _call_proxy(
            {
                "url": browser_url,
                "prime_url": "https://sportsbook.fanduel.com",
                "capture_patterns": CAPTURE_PATTERNS,
                "wait_ms": 12000,
                "timeout_ms": 60000,
            }
        )
        captured = result.get("captured_requests", [])
        logger.info(
            "Event %s: Camoufox status=%d  captured=%d",
            event_id,
            result.get("status", 0),
            len(captured),
        )
        for capture in captured:
            body = capture.get("body")
            if _is_event_page(body):
                return body
        logger.warning(
            "No event-page response captured by Camoufox for event %s", event_id
        )
    except Exception as exc:
        logger.warning("Camoufox failed for event %s: %s", event_id, exc)

    return None


# ---------------------------------------------------------------------------
# Parsing — flatten event-page markets into rows
# ---------------------------------------------------------------------------

def _parse_event_page(
    event_meta: Dict[str, Any],
    page_data: Dict[str, Any],
    scraped_at: str,
) -> List[Dict[str, Any]]:
    """Flatten all markets from an event-page response into individual rows."""
    rows: List[Dict[str, Any]] = []

    attachments = page_data.get("attachments", {})
    markets: Dict[str, Any] = attachments.get("markets", {})

    if not markets:
        return rows

    try:
        event_start = datetime.fromisoformat(
            event_meta["open_date"].replace("Z", "+00:00")
        ).strftime("%Y-%m-%dT%H:%M:%S")
    except Exception:
        event_start = None

    # Split "Player A v Player B"
    parts = event_meta["event_name"].split(" v ", 1)
    player_home = parts[0].strip() if len(parts) == 2 else event_meta["event_name"]
    player_away = parts[1].strip() if len(parts) == 2 else ""

    for market_id, market in markets.items():
        if not isinstance(market, dict):
            continue
        if market.get("marketStatus") != "OPEN":
            continue

        market_type = market.get("marketType", "")
        market_name = market.get("marketName", "")
        sgm_market = market.get("sgmMarket", False)
        in_play = market.get("inPlay", False)
        sort_priority = market.get("sortPriority")

        for runner in market.get("runners", []):
            if not isinstance(runner, dict):
                continue

            selection_id = str(runner.get("selectionId", ""))
            selection_name = runner.get("runnerName", "")
            runner_status = runner.get("runnerStatus", "")
            handicap = runner.get("handicap")
            sort_order = runner.get("sortPriority")

            win_odds = runner.get("winRunnerOdds", {})
            true_odds = win_odds.get("trueOdds", {})
            dec_obj = true_odds.get("decimalOdds", {})
            odds_decimal = dec_obj.get("decimalOdds") if isinstance(dec_obj, dict) else None

            am_obj = win_odds.get("americanDisplayOdds", {})
            odds_american = am_obj.get("americanOdds") if isinstance(am_obj, dict) else None

            # Previous odds (line movement — FanDuel returns this for free)
            prev_list = runner.get("previousWinRunnerOdds", [])
            prev_am = None
            if prev_list and isinstance(prev_list[0], dict):
                prev_am_obj = prev_list[0].get("americanDisplayOdds", {})
                prev_am = prev_am_obj.get("americanOdds")

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
                    "league": "ATP",
                    "tournament_name": event_meta["tournament_name"],
                    "competition_id": event_meta["competition_id"],
                    "event_id": event_meta["event_id"],
                    "event_name": event_meta["event_name"],
                    "player_home": player_home or None,
                    "player_away": player_away or None,
                    "event_start": event_start,
                    "is_inplay": in_play,
                    "market_id": market_id or None,
                    "market_type": market_type or None,
                    "market_name": market_name or None,
                    "market_sort_priority": sort_priority,
                    "sgm_market": sgm_market,
                    "selection_id": selection_id or None,
                    "selection_name": selection_name or None,
                    "runner_status": runner_status or None,
                    "runner_sort_priority": sort_order,
                    "handicap": float(handicap) if handicap is not None else None,
                    "odds_decimal": float(odds_decimal) if odds_decimal is not None else None,
                    "odds_american": int(odds_american) if odds_american is not None else None,
                    "odds_american_prev": int(prev_am) if prev_am is not None else None,
                    "deep_link": deep_link,
                }
            )

    return rows


# ---------------------------------------------------------------------------
# Scrape phase
# ---------------------------------------------------------------------------

def scrape(dry_run: bool = False) -> List[Dict[str, Any]]:
    scraped_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    all_rows: List[Dict[str, Any]] = []

    atp_events = fetch_atp_events_via_camoufox()
    if not atp_events:
        logger.warning("No ATP events found — nothing to scrape")
        return []

    for i, event_meta in enumerate(atp_events, 1):
        logger.info(
            "[%d/%d] %s (%s)",
            i,
            len(atp_events),
            event_meta["event_name"],
            event_meta["tournament_name"],
        )
        page_data = fetch_event_page(event_meta)
        if page_data:
            rows = _parse_event_page(event_meta, page_data, scraped_at)
            logger.info("  → %d rows", len(rows))
            all_rows.extend(rows)
        else:
            logger.warning(
                "  → No market data for event %s", event_meta["event_id"]
            )

        if i < len(atp_events):
            time.sleep(EVENT_PAGE_DELAY_S)

    logger.info("Total rows scraped: %d", len(all_rows))

    if dry_run:
        for row in all_rows[:5]:
            print(json.dumps(row, default=str))
        return all_rows

    Path(ARTIFACT_PATH).parent.mkdir(parents=True, exist_ok=True)
    with open(ARTIFACT_PATH, "w") as fh:
        for row in all_rows:
            fh.write(json.dumps(row, default=str) + "\n")
    logger.info("Wrote %d rows to %s", len(all_rows), ARTIFACT_PATH)
    return all_rows


# ---------------------------------------------------------------------------
# Load phase
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
    parser = argparse.ArgumentParser(description="FanDuel ATP tennis market scraper")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--scrape-only", action="store_true")
    group.add_argument("--load-only", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if args.load_only:
        load()
    else:
        scrape(dry_run=args.dry_run)
        if not args.scrape_only and not args.dry_run:
            load()


if __name__ == "__main__":
    main()
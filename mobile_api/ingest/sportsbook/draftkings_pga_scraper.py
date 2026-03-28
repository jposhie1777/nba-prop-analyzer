"""
DraftKings PGA Tour Market Scraper.

Discovery findings (2026-03-28):
  - League ID for PGA Tour: 84240 (confirmed from logos manifest + Nash URLs)
  - Odds come from Nash sportscontent API, NOT lineups/leagues endpoints
  - Two key Nash endpoints discovered:
    [07] sportscontent/controldata/home/marketTypeGrid/v1/markets
    [08] sportscontent/controldata/home/primaryMarkets/v1/markets
  - Both return: {sports, leagues, events, markets, selections, subscriptionPartials}
  - The scrape URL should be the PGA Tour league hub (not a specific tournament)

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

# PGA Tour league ID — confirmed from browser devtools 2026-03-28
# URL: sportsbook-nash.draftkings.com/.../markets?eventsQuery=leagueId eq '91880'
DK_PGA_LEAGUE_ID = 91880  # placeholder — will be replaced by manifest lookup

# Logos manifest — used to dynamically confirm league ID is still current
DK_LOGOS_MANIFEST = "https://sportsbook.draftkings.com/static/logos/provider/2/logos.json"

# Nash sportscontent endpoints — discovered in network capture
DK_NASH_BASE = "https://sportsbook-nash.draftkings.com"
DK_NASH_PRIMARY_MARKETS = (
    "{base}/sites/US-NJ-SB/api/sportscontent/controldata/home/primaryMarkets/v1/markets"
    "?eventsQuery=%24filter%3DleagueId%20eq%20%27{league_id}%27"
    "&marketsQuery=%24filter%3Dtags%2Fany%28t%3A%20t%20eq%20%27PrimaryMarket%27%29"
    "&top=100&include=Events&entity=events&isBatchable=true"
)

# Confirmed working from browser devtools 2026-03-28
DK_NASH_LEAGUE_MARKETS = (
    "{base}/sites/US-NJ-SB/api/sportscontent/controldata/league/leagueSubcategory/v1/markets"
    "?isBatchable=false&templateVars={league_id}%2C4508"
    "&eventsQuery=%24filter%3DleagueId%20eq%20%27{league_id}%27"
    "%20AND%20clientMetadata%2FSubcategories%2Fany%28s%3A%20s%2FId%20eq%20%274508%27%29"
    "&marketsQuery=%24filter%3DclientMetadata%2FsubCategoryId%20eq%20%274508%27"
    "%20AND%20tags%2Fall%28t%3A%20t%20ne%20%27SportcastBetBuilder%27%29"
    "&include=Events&entity=events"
)
DK_NASH_ALL_MARKETS = (
    "{base}/sites/US-NJ-SB/api/sportscontent/controldata/home/marketTypeGrid/v1/markets"
    "?eventsQuery=%24filter%3DleagueId%20eq%20%27{league_id}%27"
    "&include=Events&entity=events&isBatchable=true"
)

SCRAPE_CONFIG = {
    # PGA Tour league hub page — confirmed to trigger Nash sportscontent XHRs
    "url": "https://sportsbook.draftkings.com/leagues/golf/pga-tour",
    "prime_url": "https://sportsbook.draftkings.com/leagues/golf/pga-tour",
    "capture_patterns": [
        "sportsbook-nash.draftkings.com/sites/",
    ],
    # Increased to 35s — sportscontent XHRs fire after layout loads
    "wait_ms": 35000,
}

DISCOVER_PATTERNS = [
    "api.draftkings.com",
    "sportsbook-nash.draftkings.com",
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


_REQ_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}


# ---------------------------------------------------------------------------
# League ID discovery via logos manifest
# ---------------------------------------------------------------------------

def _confirm_league_id() -> Optional[int]:
    """
    Fetch logos manifest and find the PGA Tour league ID.
    Logs raw entry structure to help debug key names.
    Returns the PGA Tour league ID or None if not found.
    """
    GOLF_KEYWORDS = ("golf", "pga", "lpga", "tour championship", "masters", "open championship")
    try:
        resp = requests.get(DK_LOGOS_MANIFEST, headers=_REQ_HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        # Log top-level keys to understand manifest structure
        logger.info("Logos manifest top-level keys: %s", list(data.keys())[:10])

        event_groups = data.get("Eventgroups", [])
        logger.info("Logos manifest: found %d event groups", len(event_groups))

        # Log raw first entry so we can see the actual key names
        if event_groups:
            first = event_groups[0]
            logger.info("First entry keys: %s", list(first.keys()))
            logger.info("First entry sample: %s", str(first)[:400])

        golf_matches = []
        for eg in event_groups:
            if not isinstance(eg, dict):
                continue
            # Try every plausible key variant for ID
            eg_id = (eg.get("EventgroupId") or eg.get("eventGroupId") or
                     eg.get("LeagueId") or eg.get("leagueId") or
                     eg.get("id") or eg.get("Id") or eg.get("ID"))
            # Concatenate all string values and search for golf keywords
            all_values = " ".join(str(v) for v in eg.values() if isinstance(v, str))
            if eg_id and any(kw in all_values.lower() for kw in GOLF_KEYWORDS):
                logger.info("Golf match: id=%s values=%s", eg_id, all_values[:200])
                golf_matches.append((int(eg_id), all_values[:80]))

        if golf_matches:
            best = golf_matches[0]
            logger.info("PGA/Golf event group found: id=%d", best[0])
            return best[0]

        logger.warning("No golf/PGA event group found in logos manifest")
    except Exception as exc:
        logger.warning("Could not fetch logos manifest: %s", exc)
    return None


# ---------------------------------------------------------------------------
# Nash sportscontent API direct fetch
# ---------------------------------------------------------------------------

def _fetch_nash_direct(league_id: int, scraped_at: str) -> List[Dict[str, Any]]:
    """
    Fetch odds directly from the Nash sportscontent API.
    This is the API confirmed by network capture — no browser TLS required.
    """
    urls_to_try = [
        DK_NASH_LEAGUE_MARKETS.format(base=DK_NASH_BASE, league_id=league_id),
        DK_NASH_PRIMARY_MARKETS.format(base=DK_NASH_BASE, league_id=league_id),
        DK_NASH_ALL_MARKETS.format(base=DK_NASH_BASE, league_id=league_id),
    ]

    for url in urls_to_try:
        logger.info("DraftKings PGA: Nash sportscontent → %s", url[:120])
        try:
            resp = requests.get(url, headers=_REQ_HEADERS, timeout=30)
            resp.raise_for_status()
            body = resp.json()
            if _is_nash_sportscontent_response(body):
                rows = _parse_nash_sportscontent(body, scraped_at=scraped_at)
                logger.info("DraftKings PGA: Nash returned %d rows from %s", len(rows), url[:80])
                if rows:
                    return rows
            else:
                logger.warning(
                    "DraftKings PGA: Nash response doesn't look like sportscontent (keys: %s)",
                    list(body.keys())[:8] if isinstance(body, dict) else type(body).__name__,
                )
        except Exception as exc:
            logger.warning("DraftKings PGA: Nash direct fetch failed for %s: %s", url[:80], exc)

    return []


# ---------------------------------------------------------------------------
# Response validation
# ---------------------------------------------------------------------------

def _is_nash_sportscontent_response(body: Any) -> bool:
    """Check for the Nash sportscontent response shape discovered in network capture."""
    if not isinstance(body, dict):
        return False
    return bool(
        body.get("markets") or body.get("selections") or body.get("events")
    )


# ---------------------------------------------------------------------------
# Parsing — Nash sportscontent format
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
    if "3 ball" in n or "3ball" in n:
        return "matchup"
    return "other"


def _parse_nash_sportscontent(body: Dict[str, Any], scraped_at: str) -> List[Dict[str, Any]]:
    """
    Parse the Nash sportscontent API response.

    Structure (from discovery):
    {
      "events": {eventId: {id, name, startDate, ...}},
      "markets": {marketId: {id, name, eventId, marketTypeId, ...}},
      "selections": {selectionId: {id, label, marketId, trueOdds, displayOdds, ...}},
      "leagues": {...},
      "sports": {...}
    }
    """
    rows: List[Dict[str, Any]] = []
    raw_str = json.dumps(body)[:64000]

    # Nash returns dicts keyed by ID, or lists — handle both
    raw_events = body.get("events", {})
    raw_markets = body.get("markets", {})
    raw_selections = body.get("selections", {})

    # Normalize to dicts keyed by id
    def _to_dict(raw: Any, id_field: str = "id") -> Dict[str, Any]:
        if isinstance(raw, dict):
            return raw
        if isinstance(raw, list):
            return {str(item.get(id_field, i)): item for i, item in enumerate(raw) if isinstance(item, dict)}
        return {}

    events = _to_dict(raw_events)
    markets = _to_dict(raw_markets)
    selections = _to_dict(raw_selections)

    logger.info(
        "DraftKings PGA: parsing %d events, %d markets, %d selections",
        len(events), len(markets), len(selections),
    )

    # Build market → event lookup
    market_event: Dict[str, str] = {}
    for mid, mkt in markets.items():
        if isinstance(mkt, dict):
            eid = str(mkt.get("eventId") or mkt.get("event_id") or "")
            if eid:
                market_event[str(mid)] = eid

    # Walk selections
    for sel_id, sel in selections.items():
        if not isinstance(sel, dict):
            continue

        selection_id = str(sel.get("id") or sel.get("selectionId") or sel_id)
        market_id = str(sel.get("marketId") or sel.get("market_id") or "")
        outcome_label = sel.get("label") or sel.get("name") or ""

        # Odds — Nash uses trueOdds (decimal) and/or displayOdds (american string)
        odds_dec: Optional[float] = None
        odds_am: Optional[str] = None

        true_odds = sel.get("trueOdds") or sel.get("odds") or sel.get("decimalOdds")
        display_odds = sel.get("displayOdds") or sel.get("americanOdds") or sel.get("oddsAmerican")

        if true_odds is not None:
            try:
                odds_dec = float(true_odds)
            except Exception:
                pass
        if display_odds is not None:
            odds_am = str(display_odds)
            # If we have american but not decimal, derive decimal
            if odds_dec is None:
                try:
                    am_int = int(str(display_odds).replace("+", ""))
                    odds_dec = round(am_int / 100 + 1, 4) if am_int > 0 else round(100 / (-am_int) + 1, 4)
                except Exception:
                    pass

        # Handicap/line
        line_val = sel.get("points") or sel.get("line") or sel.get("handicap")
        try:
            outcome_line = float(line_val) if line_val is not None else None
        except Exception:
            outcome_line = None

        # Market info
        mkt = markets.get(market_id, {}) if market_id else {}
        market_name = (mkt.get("name") or mkt.get("label") or "") if isinstance(mkt, dict) else ""
        market_type = _classify_market(market_name)
        market_type_id = str(mkt.get("marketTypeId") or "") if isinstance(mkt, dict) else ""

        # Event info
        event_id = market_event.get(market_id, "")
        ev = events.get(event_id, {}) if event_id else {}
        ev_name = (ev.get("name") or ev.get("eventName") or "") if isinstance(ev, dict) else ""
        start_raw = (ev.get("startDate") or ev.get("startDateTime") or "") if isinstance(ev, dict) else ""
        try:
            event_start = (
                datetime.fromisoformat(start_raw.replace("Z", "+00:00")).strftime("%Y-%m-%dT%H:%M:%S")
                if start_raw else None
            )
        except Exception:
            event_start = None

        # Deep link
        deep_link = f"dksb://sb/addbet/{selection_id}" if selection_id else None

        rows.append({
            "scraped_at": scraped_at,
            "event_id": event_id or None,
            "event_name": ev_name or None,
            "event_start": event_start,
            "market_id": market_id or None,
            "market_name": market_name or None,
            "market_type_id": market_type_id or None,
            "market_type": market_type,
            "selection_id": selection_id or None,
            "selection_label": outcome_label or None,
            "outcome_line": outcome_line,
            "odds_decimal": odds_dec,
            "odds_american": odds_am,
            "deep_link": deep_link,
            "raw_response": raw_str,
        })

    return rows


def _parse_captured(captured: List[Dict[str, Any]], scraped_at: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for i, capture in enumerate(captured):
        url = capture.get("url", "<unknown>")
        body = capture.get("body")
        logger.info("  captured[%d]: %s", i, url)
        if not _is_nash_sportscontent_response(body):
            logger.debug("  → skipping")
            continue
        new_rows = _parse_nash_sportscontent(body, scraped_at=scraped_at)
        logger.info("  → sportscontent: %d rows", len(new_rows))
        rows.extend(new_rows)
    logger.info(
        "DraftKings PGA: %d captured, %d total rows from browser capture",
        len(captured), len(rows),
    )
    return rows


# ---------------------------------------------------------------------------
# Discovery mode
# ---------------------------------------------------------------------------

def discover() -> None:
    logger.info("DISCOVERY MODE — capturing all api/nash requests")
    result = _call_proxy({
        "url": SCRAPE_CONFIG["url"],
        "prime_url": SCRAPE_CONFIG["prime_url"],
        "capture_patterns": DISCOVER_PATTERNS,
        "wait_ms": SCRAPE_CONFIG["wait_ms"],
        "timeout_ms": 90000,
    })

    captured = result.get("captured_requests", [])
    logger.info("page_status=%d  total_captured=%d", result.get("status", 0), len(captured))

    eg_pattern = re.compile(r"/leagues?/(\d+)")
    found_ids = set()

    print("\n=== DISCOVERED URLs ===")
    for i, capture in enumerate(captured):
        url = capture.get("url", "<unknown>")
        body = capture.get("body")
        top_keys = list(body.keys())[:6] if isinstance(body, dict) else "n/a"
        print(f"[{i:02d}] {url}")
        print(f"      body_type={type(body).__name__}  top_keys={top_keys}")
        m = eg_pattern.search(url)
        if m:
            found_ids.add(m.group(1))
    print("=== END ===\n")

    # Also check logos manifest
    logger.info("Fetching logos manifest...")
    confirmed_id = _confirm_league_id()
    if confirmed_id:
        found_ids.add(str(confirmed_id))

    if found_ids:
        print(f"LEAGUE/EVENT GROUP IDs FOUND: {sorted(found_ids)}")


# ---------------------------------------------------------------------------
# Scrape
# ---------------------------------------------------------------------------

def scrape(dry_run: bool = False) -> List[Dict[str, Any]]:
    scraped_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Step 1: Try to confirm league ID from logos manifest
    confirmed_id = _confirm_league_id()
    if confirmed_id is None:
        logger.info("DraftKings PGA: manifest lookup failed — using hardcoded league ID %d", DK_PGA_LEAGUE_ID)
        league_id = DK_PGA_LEAGUE_ID
    else:
        league_id = confirmed_id
    logger.info("DraftKings PGA: using league ID %d", league_id)

    # Step 2: Try Nash sportscontent API directly (no browser needed)
    rows = _fetch_nash_direct(league_id, scraped_at=scraped_at)

    # Step 3: Fall back to browser capture if direct API returned nothing
    if not rows:
        logger.info("DraftKings PGA: direct API returned 0 rows — trying browser capture")
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

    logger.info("DraftKings PGA: %d total rows", len(rows))

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
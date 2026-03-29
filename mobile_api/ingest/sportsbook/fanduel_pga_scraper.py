"""
FanDuel PGA Tour Market Scraper — v5 (navigation-endpoint architecture)

Architecture:
  1. Camoufox fetches the FanDuel PGA navigation endpoint as a JSON XHR:
       https://sportsbook.fanduel.com/navigation/pga
     The proxy intercepts the XHR response body, which contains:
       - All externalMarketIds in layout.coupons[*].externalMarketId
       - Player selectionId → name mapping in attachments.markets[*].runners
       - Event metadata in attachments.events
  2. Extracts all externalMarketIds from the navigation response
  3. Also builds a selectionId → player_name lookup from runners data
  4. POSTs all marketIds to getMarketPrices in batches DIRECTLY (no browser
     needed for this step — PerimeterX does not block server-side POSTs)
  5. Classifies markets and builds deep links
  6. Writes to BigQuery: sportsbook.raw_fanduel_pga_markets

Key advantages over v4:
  - Single Camoufox call (just to fetch navigation JSON) instead of waiting
    for many lazy-loaded POST captures
  - No lazy-load timing issues — navigation JSON contains ALL market IDs
    including collapsed/unloaded ones
  - No x-px-context token capture needed
  - Player name lookup included in same response
  - getMarketPrices POSTs go direct (no proxy overhead per batch)

Usage:
  python -m mobile_api.ingest.sportsbook.fanduel_pga_scraper --dry-run
  python -m mobile_api.ingest.sportsbook.fanduel_pga_scraper --scrape-only
  python -m mobile_api.ingest.sportsbook.fanduel_pga_scraper --load-only
  python -m mobile_api.ingest.sportsbook.fanduel_pga_scraper --discover
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
from typing import Any, Dict, List, Optional, Set, Tuple
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

FD_BASE_URL = "https://sportsbook.fanduel.com"

# Navigation endpoint — returns full market structure + player name mappings
FD_NAV_URL = "https://sportsbook.fanduel.com/navigation/pga"

# getMarketPrices — takes a list of externalMarketIds, returns odds
FD_MARKET_PRICES_URL = (
    "https://smp.nj.sportsbook.fanduel.com"
    "/api/sports/fixedodds/readonly/v1/getMarketPrices?priceHistory=0"
)

# Headers for getMarketPrices POST (direct, no proxy needed)
FD_API_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/130.0.0.0 Mobile/15E148 Safari/604.1"
    ),
    "Accept": "application/json",
    "Content-Type": "application/json",
    "x-sportsbook-region": "NJ",
    "X-Application": "FhMFpcPWXMeyZxOx",
}

MARKET_PRICES_BATCH_SIZE = 50

# Known static selectionIds for round score over/under markets
ROUND_SCORE_SELECTION_IDS = {
    "23730687", "23730688",
    "16274521", "16274522",
    "68613232", "23746580",
}

# Known static selectionIds for hole score birdie/par/bogey markets
HOLE_SCORE_SELECTION_IDS = {
    "61579324", "61579325", "13543690",
}

# Tabs to query on the navigation endpoint — each may expose different market sets
NAV_TABS = [
    "",                     # default / outright
    "finishing-positions",
    "matchups",
    "round-score",
    "3-balls",
    "hole-scores",
    "top-in-region",
    "groups",
]


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
    service_url = _get_camoufox_url()
    token = _get_camoufox_token()
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    resp = requests.post(
        f"{service_url}/fetch",
        json=payload,
        headers=headers,
        timeout=150,
    )
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# Navigation endpoint — fetched via Camoufox (bot-protected)
# ---------------------------------------------------------------------------

def _fetch_navigation(tab: str = "") -> Optional[Dict[str, Any]]:
    """
    Fetch the FanDuel PGA navigation JSON for a given tab via Camoufox.

    The navigation endpoint returns HTML to plain HTTP clients but serves
    JSON to authenticated browser sessions. We use Camoufox to make the
    request look like a real browser XHR and capture the JSON response body.

    The proxy payload uses `capture_patterns` to intercept the XHR that
    the page fires for navigation data, and `extract_json_url` to pull the
    response body of that specific request.
    """
    url = FD_NAV_URL
    if tab:
        url = f"{FD_NAV_URL}?tab={tab}"

    logger.info("Fetching navigation tab=%r via Camoufox → %s", tab or "default", url)

    try:
        result = _call_proxy({
            "url": url,
            "capture_patterns": ["sportsbook.fanduel.com/navigation/"],
            "wait_ms": 8000,
            "timeout_ms": 30000,
            # Tell proxy to treat this as a direct JSON fetch
            # (load the URL as if it were an XHR, return body directly)
            "as_json": True,
        })

        # The proxy may return the JSON body directly in result["body"]
        # or as the first captured request's body
        body = result.get("body")
        if isinstance(body, dict) and ("layout" in body or "attachments" in body):
            logger.info("  → JSON body from proxy response (keys=%s)", list(body.keys())[:6])
            return body

        # Check captured requests
        captured = result.get("captured_requests", [])
        for cap in captured:
            cap_url = cap.get("url", "")
            if "navigation" in cap_url:
                cap_body = cap.get("body")
                if isinstance(cap_body, dict) and (
                    "layout" in cap_body or "attachments" in cap_body
                ):
                    logger.info("  → JSON from captured request: %s", cap_url[:80])
                    return cap_body

        # If body is a string, try parsing it
        if isinstance(body, str) and body.strip().startswith("{"):
            parsed = json.loads(body)
            if isinstance(parsed, dict):
                logger.info("  → Parsed JSON string from proxy body")
                return parsed

        logger.warning(
            "  navigation tab=%r: no usable JSON in proxy response "
            "(status=%s, body_type=%s, captured=%d)",
            tab or "default",
            result.get("status"),
            type(body).__name__,
            len(captured),
        )
        return None

    except Exception as exc:
        logger.warning("  navigation fetch failed for tab=%r: %s", tab or "default", exc)
        return None


def _extract_market_ids_from_nav(data: Dict[str, Any]) -> Set[str]:
    """
    Pulls every externalMarketId out of the navigation response.

    They appear in two places:
      1. layout.coupons[coupon_id].externalMarketId   (top-level single ID per coupon)
      2. layout.coupons[coupon_id].display[*].rows[*].marketIds[]  (grouped display rows)
    """
    market_ids: Set[str] = set()
    layout = data.get("layout", {})
    coupons = layout.get("coupons", {})

    for coupon_id, coupon in coupons.items():
        if not isinstance(coupon, dict):
            continue

        # Top-level externalMarketId on coupon
        ext_id = coupon.get("externalMarketId")
        if ext_id:
            market_ids.add(str(ext_id))

        # display rows
        for display_item in coupon.get("display", []):
            if not isinstance(display_item, dict):
                continue
            for row in display_item.get("rows", []):
                if not isinstance(row, dict):
                    continue
                for mid in row.get("marketIds", []):
                    if mid:
                        market_ids.add(str(mid))

    return market_ids


def _extract_player_names_from_nav(data: Dict[str, Any]) -> Dict[str, str]:
    """
    Builds a selectionId → playerName map from the runners embedded in
    attachments.markets within the navigation response.

    This is a bonus — gives us player name lookup without an extra API call.
    """
    player_map: Dict[str, str] = {}
    attachments = data.get("attachments", {})
    markets = attachments.get("markets", {})

    for market_id, market in markets.items():
        if not isinstance(market, dict):
            continue
        for runner in market.get("runners", []):
            if not isinstance(runner, dict):
                continue
            sel_id = str(runner.get("selectionId", ""))
            name = runner.get("runnerName", "")
            if sel_id and name:
                player_map[sel_id] = name

    return player_map


def _extract_event_name_from_nav(data: Dict[str, Any]) -> str:
    """
    Pulls the current tournament event name from attachments.events.
    Returns the first golf event name found, or empty string.
    """
    attachments = data.get("attachments", {})
    events = attachments.get("events", {})
    for ev_id, ev in events.items():
        if not isinstance(ev, dict):
            continue
        name = ev.get("name", "")
        if name:
            return name.strip()
    return ""


def _extract_external_market_ids_from_nav(data: Dict[str, Any]) -> Set[str]:
    """
    Also checks attachments.markets[*].associatedMarkets[*].externalMarketId
    for any markets that were fully loaded in the navigation response.
    """
    market_ids: Set[str] = set()
    attachments = data.get("attachments", {})
    markets = attachments.get("markets", {})

    for market_id, market in markets.items():
        if not isinstance(market, dict):
            continue
        for assoc in market.get("associatedMarkets", []):
            if not isinstance(assoc, dict):
                continue
            ext_id = assoc.get("externalMarketId")
            if ext_id:
                market_ids.add(str(ext_id))

    return market_ids


def fetch_all_market_ids() -> Tuple[Set[str], Dict[str, str], str]:
    """
    Queries all navigation tabs and merges results.

    Returns:
        market_ids  - all unique externalMarketIds found
        player_map  - selectionId → playerName
        event_name  - current tournament name
    """
    all_market_ids: Set[str] = set()
    all_player_map: Dict[str, str] = {}
    event_name = ""

    for tab in NAV_TABS:
        data = _fetch_navigation(tab)
        if not data:
            logger.warning("Skipping tab=%r — no data returned", tab or "default")
            continue

        # Market IDs from coupon layout
        ids_from_layout = _extract_market_ids_from_nav(data)

        # Market IDs from fully-loaded attachments
        ids_from_attachments = _extract_external_market_ids_from_nav(data)

        tab_ids = ids_from_layout | ids_from_attachments
        logger.info(
            "Tab %-25s → layout=%d  attachments=%d  total=%d",
            repr(tab or "default"),
            len(ids_from_layout),
            len(ids_from_attachments),
            len(tab_ids),
        )
        all_market_ids |= tab_ids

        # Player names (merge — later tabs may add more players)
        player_map = _extract_player_names_from_nav(data)
        all_player_map.update(player_map)

        # Event name from first tab that has it
        if not event_name:
            event_name = _extract_event_name_from_nav(data)

        # Be polite between tab requests
        time.sleep(2)

    logger.info(
        "Total: %d unique marketIds, %d players, event=%r",
        len(all_market_ids), len(all_player_map), event_name,
    )
    return all_market_ids, all_player_map, event_name


# ---------------------------------------------------------------------------
# Market classification
# ---------------------------------------------------------------------------

def _classify_market(runners: List[Dict], turn_in_play: bool) -> str:
    n = len(runners)
    sel_ids = {str(r.get("selectionId", "")) for r in runners}

    if n == 2:
        if sel_ids & ROUND_SCORE_SELECTION_IDS:
            return "round_score"
        return "matchup"
    if n == 3:
        if sel_ids & HOLE_SCORE_SELECTION_IDS:
            return "hole_score"
        return "three_ball"
    if n >= 4:
        if not turn_in_play:
            return "finishing_position"
        return "outright_winner"
    return "other"


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
# Parse getMarketPrices response
# ---------------------------------------------------------------------------

def _parse_market_prices_response(
    body: List[Any],
    scraped_at: str,
    event_name: str = "",
    player_map: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not isinstance(body, list):
        return rows

    player_map = player_map or {}
    raw_str = json.dumps(body)[:32000]

    for market in body:
        if not isinstance(market, dict):
            continue

        market_id = str(market.get("marketId", ""))
        turn_in_play = bool(market.get("turnInPlayEnabled", True))
        inplay = bool(market.get("inplay", False))
        market_status = market.get("marketStatus", "")
        market_name = market.get("marketName") or market.get("name") or ""
        runners = market.get("runnerDetails", [])

        if not market_id or not runners:
            continue

        market_type = _classify_market(runners, turn_in_play)

        for runner in runners:
            if not isinstance(runner, dict):
                continue

            selection_id = str(runner.get("selectionId", ""))
            runner_name = runner.get("runnerName") or player_map.get(selection_id, "")
            runner_status = runner.get("runnerStatus", "")
            handicap = runner.get("handicap")
            try:
                handicap_f = float(handicap) if handicap is not None else None
            except Exception:
                handicap_f = None

            win_odds = runner.get("winRunnerOdds") or {}
            true_odds = win_odds.get("trueOdds") or {}
            dec_obj = true_odds.get("decimalOdds") or {}
            am_obj = win_odds.get("americanDisplayOdds") or {}

            odds_dec = None
            odds_am = None

            try:
                dec_val = dec_obj.get("decimalOdds")
                if dec_val is not None:
                    odds_dec = float(dec_val)
            except Exception:
                pass

            try:
                am_val = am_obj.get("americanOdds")
                if am_val is not None:
                    am_int = int(am_val)
                    odds_am = f"+{am_int}" if am_int >= 0 else str(am_int)
            except Exception:
                pass

            if odds_dec is not None and odds_am is None:
                odds_am = _american_from_decimal(odds_dec)

            deep_link = (
                _build_deep_link(market_id, selection_id)
                if market_id and selection_id else None
            )

            rows.append({
                "scraped_at": scraped_at,
                "source": "getMarketPrices",
                "event_name": event_name or None,
                "market_id": market_id,
                "market_name": market_name or None,
                "market_type": market_type,
                "market_status": market_status or None,
                "turn_in_play": turn_in_play,
                "inplay": inplay,
                "selection_id": selection_id or None,
                "player_name": runner_name or None,
                "runner_status": runner_status or None,
                "handicap": handicap_f,
                "odds_decimal": odds_dec,
                "odds_american": odds_am,
                "deep_link": deep_link,
                "raw_response": raw_str,
            })

    return rows


# ---------------------------------------------------------------------------
# getMarketPrices batched POST
# ---------------------------------------------------------------------------

def _fetch_market_prices(
    market_ids: List[str],
    scraped_at: str,
    event_name: str = "",
    player_map: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    all_rows: List[Dict[str, Any]] = []
    batches = [
        market_ids[i:i + MARKET_PRICES_BATCH_SIZE]
        for i in range(0, len(market_ids), MARKET_PRICES_BATCH_SIZE)
    ]

    for i, batch in enumerate(batches):
        logger.info(
            "getMarketPrices batch %d/%d: %d marketIds",
            i + 1, len(batches), len(batch),
        )
        try:
            resp = requests.post(
                FD_MARKET_PRICES_URL,
                json={"marketIds": batch},
                headers=FD_API_HEADERS,
                timeout=30,
            )
            resp.raise_for_status()
            body = resp.json()
            rows = _parse_market_prices_response(
                body, scraped_at, event_name, player_map
            )
            logger.info("  → %d rows", len(rows))
            all_rows.extend(rows)
        except Exception as exc:
            logger.warning("Batch %d failed: %s", i + 1, exc)

        # Small delay between batches to be polite
        if i < len(batches) - 1:
            time.sleep(0.5)

    return all_rows


# ---------------------------------------------------------------------------
# Discover mode — print all found marketIds and player counts per tab
# ---------------------------------------------------------------------------

def discover() -> None:
    logger.info("DISCOVERY MODE — querying all navigation tabs")
    for tab in NAV_TABS:
        data = _fetch_navigation(tab)
        if not data:
            print(f"[{tab or 'default':30s}] FAILED")
            continue

        layout_ids = _extract_market_ids_from_nav(data)
        attach_ids = _extract_external_market_ids_from_nav(data)
        players = _extract_player_names_from_nav(data)
        event_name = _extract_event_name_from_nav(data)

        print(
            f"[{tab or 'default':30s}] "
            f"layout_ids={len(layout_ids):4d}  "
            f"attach_ids={len(attach_ids):4d}  "
            f"players={len(players):4d}  "
            f"event={event_name!r}"
        )
        time.sleep(1)

    print("\nSample from default tab:")
    data = _fetch_navigation("")
    if data:
        ids = _extract_market_ids_from_nav(data)
        players = _extract_player_names_from_nav(data)
        print(f"  First 10 marketIds: {sorted(ids)[:10]}")
        print(f"  Sample players: {dict(list(players.items())[:10])}")


# ---------------------------------------------------------------------------
# Scrape
# ---------------------------------------------------------------------------

def scrape(dry_run: bool = False) -> List[Dict[str, Any]]:
    scraped_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Step 1: fetch all market IDs and player name map from navigation endpoint
    market_ids, player_map, event_name = fetch_all_market_ids()

    if not market_ids:
        logger.error("No marketIds found from navigation endpoint. Aborting.")
        return []

    logger.info(
        "Discovered %d marketIds for %r (%d players in name map)",
        len(market_ids), event_name, len(player_map),
    )

    # Step 2: fetch odds for all markets
    market_id_list = sorted(market_ids)
    rows = _fetch_market_prices(
        market_id_list,
        scraped_at=scraped_at,
        event_name=event_name,
        player_map=player_map,
    )

    # Step 3: summary
    by_type: Dict[str, int] = {}
    for row in rows:
        mt = row.get("market_type", "other")
        by_type[mt] = by_type.get(mt, 0) + 1

    logger.info("FanDuel PGA: %d total rows across %d market types", len(rows), len(by_type))
    for mt, count in sorted(by_type.items(), key=lambda x: -x[1]):
        logger.info("  %-25s %d", mt, count)

    if not rows:
        logger.warning("0 rows parsed — check if getMarketPrices endpoint is accessible.")
        return []

    if dry_run:
        seen: Set[str] = set()
        for row in rows:
            mt = row.get("market_type", "")
            if mt not in seen:
                seen.add(mt)
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
        logger.warning("No artifact at %s", ARTIFACT_PATH)
        return

    rows = []
    with open(ARTIFACT_PATH) as fh:
        for line in fh:
            line = line.strip()
            if line:
                rows.append(json.loads(line))

    if not rows:
        logger.info("Artifact empty — nothing to load")
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
    parser = argparse.ArgumentParser(description="FanDuel PGA Tour market scraper v5")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--scrape-only", action="store_true", help="Scrape and write artifact, skip BQ load")
    group.add_argument("--load-only", action="store_true", help="Load existing artifact to BigQuery")
    group.add_argument("--discover", action="store_true", help="Print market/player counts per nav tab")
    parser.add_argument("--dry-run", action="store_true", help="Scrape and print sample rows, skip writes")
    args = parser.parse_args()

    if args.load_only:
        load()
    elif args.discover:
        discover()
    else:
        rows = scrape(dry_run=args.dry_run)
        if not args.scrape_only and not args.dry_run and rows:
            load()


if __name__ == "__main__":
    main()
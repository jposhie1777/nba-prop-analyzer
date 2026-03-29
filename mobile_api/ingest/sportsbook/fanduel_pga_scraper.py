"""
FanDuel PGA Tour Market Scraper — v4 (capture-all-POSTs architecture)

Architecture (confirmed from Eruda mobile network capture 2026-03-28):

  1. Camoufox loads the current tournament page (URL discovered dynamically)
  2. Captures ALL getMarketPrices POST requests fired by the page
     - Extracts every marketId from each POST body
     - Captures the x-px-context PerimeterX token from request headers
  3. POSTs ALL discovered marketIds to getMarketPrices in one batch
     - Uses the captured x-px-context token for auth
  4. Classifies markets by runner count:
       2  runners + static IDs → round_score
       2  runners              → matchup
       3  runners + static IDs → hole_score
       3  runners              → three_ball
       10+ runners, no in-play → finishing_position (top 5/10/20)
       10+ runners, in-play    → outright_winner
  5. Builds deep links: fanduelsportsbook://launch?deepLink=addToBetslip%3F...
  6. Writes to BigQuery: sportsbook.raw_fanduel_pga_markets

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

FD_MARKET_PRICES_URL = (
    "https://smp.nj.sportsbook.fanduel.com"
    "/api/sports/fixedodds/readonly/v1/getMarketPrices?priceHistory=0"
)

FD_APP_CONTEXT_URL = (
    "https://api.sportsbook.fanduel.com/sbapi/application-context"
    "?dataEntries=POPULAR_BETTING,QUICK_LINKS,AZ_BETTING,EVENT_TYPES,TEASER_COMPS"
    "&_ak=FhMFpcPWXMeyZxOx"
)
FD_GOLF_EVENT_TYPE_ID = "3"

FD_API_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/130.0.0.0 Mobile/15E148 Safari/604.1"
    ),
    "Accept": "application/json",
    "x-sportsbook-region": "IA",
    "X-Application": "FhMFpcPWXMeyZxOx",
    "Content-Type": "application/json",
}

MARKET_PRICES_BATCH_SIZE = 50

SCRAPE_CONFIG = {
    "url": f"{FD_BASE_URL}/golf",
    "prime_url": f"{FD_BASE_URL}/golf",
    "capture_patterns": [
        "smp.ia.sportsbook.fanduel.com/api/sports/fixedodds",
        "smp.nj.sportsbook.fanduel.com/api/sports/fixedodds",
        "api.sportsbook.fanduel.com/sbapi/application-context",
    ],
    "wait_ms": 30000,
}

DISCOVER_PATTERNS = ["fanduel.com", "api.", "smp."]

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
        timeout=150,
    )
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# Tournament URL discovery
# ---------------------------------------------------------------------------

def _extract_tournament_url_from_context(data: Dict[str, Any]) -> Optional[str]:
    events = data.get("events") or {}
    logger.info("application-context events: %d", len(events))
    best_event = None
    best_start = ""
    for ev_id, ev in events.items():
        if not isinstance(ev, dict):
            continue
        if str(ev.get("eventTypeId", "")) != FD_GOLF_EVENT_TYPE_ID:
            continue
        seo = ev.get("seoIdentifier") or ev.get("slug") or ""
        start = ev.get("openDate") or ev.get("startTime") or ""
        name = ev.get("eventName") or ev.get("name") or ""
        logger.info("  golf event: id=%s name=%s seo=%s", ev_id, name, seo)
        if seo and ev_id:
            if best_start == "" or start > best_start:
                best_start = start
                best_event = (ev_id, seo, name)
    if best_event:
        ev_id, seo, name = best_event
        url = f"{FD_BASE_URL}/golf/{seo}-{ev_id}"
        logger.info("Tournament URL: %s (%s)", url, name)
        return url
    return None


def _discover_tournament_url() -> str:
    fallback = f"{FD_BASE_URL}/golf"
    try:
        resp = requests.get(FD_APP_CONTEXT_URL, headers=FD_API_HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        url = _extract_tournament_url_from_context(data)
        if url:
            return url
        logger.warning("No golf event in application-context — using fallback")
    except Exception as exc:
        logger.warning("Tournament discovery failed: %s — using fallback", exc)

    # Known fallback for Houston Open 2026
    return "https://sportsbook.fanduel.com/golf/texas-children%27s-houston-open-35406067"


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
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not isinstance(body, list):
        return rows

    raw_str = json.dumps(body)[:32000]

    for market in body:
        if not isinstance(market, dict):
            continue

        market_id = str(market.get("marketId", ""))
        turn_in_play = bool(market.get("turnInPlayEnabled", True))
        inplay = bool(market.get("inplay", False))
        market_status = market.get("marketStatus", "")
        runners = market.get("runnerDetails", [])

        if not market_id or not runners:
            continue

        market_type = _classify_market(runners, turn_in_play)

        for runner in runners:
            if not isinstance(runner, dict):
                continue

            selection_id = str(runner.get("selectionId", ""))
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
                "market_type": market_type,
                "market_status": market_status or None,
                "turn_in_play": turn_in_play,
                "inplay": inplay,
                "selection_id": selection_id or None,
                "runner_status": runner_status or None,
                "handicap": handicap_f,
                "odds_decimal": odds_dec,
                "odds_american": odds_am,
                "deep_link": deep_link,
                "raw_response": raw_str,
            })

    return rows


# ---------------------------------------------------------------------------
# Direct getMarketPrices POST
# ---------------------------------------------------------------------------

def _fetch_market_prices(
    market_ids: List[str],
    px_context: str,
    scraped_at: str,
    event_name: str = "",
) -> List[Dict[str, Any]]:
    all_rows: List[Dict[str, Any]] = []
    batches = [
        market_ids[i:i + MARKET_PRICES_BATCH_SIZE]
        for i in range(0, len(market_ids), MARKET_PRICES_BATCH_SIZE)
    ]

    headers = {**FD_API_HEADERS}
    if px_context:
        headers["x-px-context"] = px_context

    for i, batch in enumerate(batches):
        logger.info(
            "getMarketPrices batch %d/%d: %d marketIds",
            i + 1, len(batches), len(batch),
        )
        try:
            resp = requests.post(
                FD_MARKET_PRICES_URL,
                json={"marketIds": batch},
                headers=headers,
                timeout=30,
            )
            resp.raise_for_status()
            body = resp.json()
            rows = _parse_market_prices_response(body, scraped_at, event_name)
            logger.info("  → %d rows", len(rows))
            all_rows.extend(rows)
        except Exception as exc:
            logger.warning("Batch %d failed: %s", i + 1, exc)

    return all_rows


# ---------------------------------------------------------------------------
# Browser capture extraction
# ---------------------------------------------------------------------------

def _extract_from_captures(
    captured: List[Dict[str, Any]],
) -> Tuple[Set[str], str, str]:
    """
    Returns: (market_ids, px_context, event_name)
    """
    market_ids: Set[str] = set()
    px_context = ""
    event_name = ""

    for i, capture in enumerate(captured):
        url = capture.get("url", "")
        body = capture.get("body")
        req_headers = (
            capture.get("request_headers")
            or capture.get("requestHeaders")
            or {}
        )

        logger.info("  captured[%d]: %s", i, url[:100])

        # Grab x-px-context from any request headers
        if not px_context and isinstance(req_headers, dict):
            for hk, hv in req_headers.items():
                if hk.lower() in ("x-px-context",):
                    px_context = str(hv)
                    logger.info("  → x-px-context captured (%d chars)", len(px_context))
                    break

        # Extract marketIds from getMarketPrices POST bodies
        if "getMarketPrices" in url:
            if isinstance(body, dict) and "marketIds" in body:
                ids = [str(m) for m in body["marketIds"] if m]
                market_ids.update(ids)
                logger.info("  → POST body: %d marketIds", len(ids))
            elif isinstance(body, list):
                # response body — grab marketIds from there too
                for mkt in body:
                    if isinstance(mkt, dict) and mkt.get("marketId"):
                        market_ids.add(str(mkt["marketId"]))

        # Extract event name from application-context
        if "application-context" in url and isinstance(body, dict) and not event_name:
            ctx_url = _extract_tournament_url_from_context(body)
            if ctx_url:
                parts = ctx_url.rstrip("/").split("/")
                if parts:
                    raw = parts[-1]
                    segs = raw.split("-")
                    if segs and segs[-1].isdigit():
                        segs = segs[:-1]
                    event_name = " ".join(p.title() for p in segs)

    logger.info(
        "Extraction complete: %d marketIds, px_context=%s, event=%s",
        len(market_ids),
        "YES" if px_context else "NO",
        event_name or "unknown",
    )
    return market_ids, px_context, event_name


# ---------------------------------------------------------------------------
# Discovery mode
# ---------------------------------------------------------------------------

def discover() -> None:
    tournament_url = _discover_tournament_url()
    logger.info("DISCOVERY MODE → %s", tournament_url)
    result = _call_proxy({
        "url": tournament_url,
        "prime_url": tournament_url,
        "capture_patterns": DISCOVER_PATTERNS,
        "wait_ms": SCRAPE_CONFIG["wait_ms"],
        "timeout_ms": 120000,
    })
    captured = result.get("captured_requests", [])
    logger.info("page_status=%d  total_captured=%d", result.get("status", 0), len(captured))
    print("\n=== DISCOVERED URLs ===")
    for i, cap in enumerate(captured):
        url = cap.get("url", "<unknown>")
        body = cap.get("body")
        body_type = type(body).__name__
        top_keys = list(body.keys())[:6] if isinstance(body, dict) else "n/a"
        has_rh = bool(cap.get("request_headers") or cap.get("requestHeaders"))
        print(f"[{i:02d}] {url[:120]}")
        print(f"      body_type={body_type}  top_keys={top_keys}  has_req_headers={has_rh}")
    print("=== END ===\n")


# ---------------------------------------------------------------------------
# Scrape
# ---------------------------------------------------------------------------

def scrape(dry_run: bool = False) -> List[Dict[str, Any]]:
    scraped_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Step 1: discover tournament URL
    tournament_url = _discover_tournament_url()
    logger.info("Loading: %s", tournament_url)

    # Step 2: Camoufox browser load — captures getMarketPrices POSTs
    result = _call_proxy({
        "url": tournament_url,
        "prime_url": tournament_url,
        "capture_patterns": SCRAPE_CONFIG["capture_patterns"],
        "wait_ms": SCRAPE_CONFIG["wait_ms"],
        "timeout_ms": 120000,
    })

    captured = result.get("captured_requests", [])
    logger.info(
        "page_status=%d  captured_requests=%d",
        result.get("status", 0), len(captured),
    )

    # Step 3: extract marketIds + token
    market_ids, px_context, event_name = _extract_from_captures(captured)

    if not market_ids:
        logger.warning("No marketIds captured. Run --discover to debug.")
        return []

    if not px_context:
        logger.warning(
            "No x-px-context captured — PerimeterX may block direct POSTs. "
            "Check if Camoufox is forwarding request headers."
        )

    # Step 4: POST all marketIds to get full odds
    market_id_list = sorted(market_ids)
    logger.info("Fetching odds for %d markets...", len(market_id_list))
    rows = _fetch_market_prices(
        market_id_list,
        px_context=px_context,
        scraped_at=scraped_at,
        event_name=event_name,
    )

    # Step 5: log summary by market type
    by_type: Dict[str, int] = {}
    for row in rows:
        mt = row.get("market_type", "other")
        by_type[mt] = by_type.get(mt, 0) + 1

    logger.info("FanDuel PGA: %d rows, %d market types", len(rows), len(by_type))
    for mt, count in sorted(by_type.items(), key=lambda x: -x[1]):
        logger.info("  %-25s %d", mt, count)

    if not rows:
        logger.warning("0 rows parsed.")

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
        logger.info("Artifact empty")
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
    parser = argparse.ArgumentParser(description="FanDuel PGA Tour market scraper v4")
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
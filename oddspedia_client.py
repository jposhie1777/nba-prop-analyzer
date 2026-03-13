#oddspedia_client.py
"""
Oddspedia odds scraper.

Fetches the Oddspedia odds page for a given sport/URL and returns a clean list
of matches with their odds.

Live URL scraping uses Playwright (headless Chromium) to bypass bot-detection
and reads window.__NUXT__ directly from the browser context.

File-based scraping (for local testing) evaluates the embedded Nuxt SSR state
via a Node.js subprocess.

Requirements:
  Live scraping : playwright Python package + `playwright install chromium`
  File scraping : Node.js >=14

Usage:
    from oddspedia_client import OddspediaClient

    client = OddspediaClient()

    # From a live URL (uses Playwright)
    matches = client.scrape("https://www.oddspedia.com/us/tennis/odds")

    # From a saved HTML file (uses Node.js, no browser needed)
    matches = client.scrape_file("website_responses/oddspedia/test")
"""

from __future__ import annotations

import json
import logging
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

LOGGER = logging.getLogger(__name__)

MARKET_NAMES: Dict[str, str] = {
    "201": "moneyline",
    "301": "spread",
}

_NODE_EXTRACTOR = r"""
const fs   = require('fs');
const vm   = require('vm');
const html = fs.readFileSync(process.argv[2], 'utf8');

const m = html.match(/window\.__NUXT__\s*=\s*([\s\S]*?)<\/script>/);
if (!m) { process.stderr.write('__NUXT__ block not found\n'); process.exit(1); }

const nuxt  = vm.runInContext(m[1].trim().replace(/;$/, ''), vm.createContext({}));
const data0 = nuxt.data[0];

process.stdout.write(JSON.stringify({
    matchList : data0.matchList  || [],
    odds      : data0.odds       || {},
    sport     : (data0.currentSport || {}).slug || null,
}));
"""


class OddspediaClient:

    def __init__(
        self,
        user_agent: str = (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        page_timeout_ms: int = 60000,
    ) -> None:
        self._user_agent = user_agent
        self._page_timeout_ms = page_timeout_ms


    # ============================================================
    # MAIN SCRAPER
    # ============================================================

    def scrape(self, url: str) -> List[Dict[str, Any]]:

        from playwright.sync_api import sync_playwright

        try:
            from playwright_stealth import stealth_sync as _stealth_sync
        except ImportError:
            _stealth_sync = None

        LOGGER.info("Fetching %s via Playwright", url)

        with sync_playwright() as pw:

            browser = pw.chromium.launch(headless=True)

            context = browser.new_context(
                user_agent=self._user_agent,
                locale="en-US",
                viewport={"width": 1920, "height": 1080},
                extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
            )

            page = context.new_page()

            if _stealth_sync:
                _stealth_sync(page)

            # ---------------------------------------------------
            # API RESPONSE CACHE
            # ---------------------------------------------------

            api_cache: Dict[str, Any] = {}

            def handle_response(resp):

                if "getMatchOdds" not in resp.url:
                    return

                if resp.status != 200:
                    return

                try:
                    data = resp.json()
                    mid = data.get("data", {}).get("match_id")

                    if mid:
                        api_cache[str(mid)] = data

                except Exception:
                    pass

            page.on("response", handle_response)

            # ---------------------------------------------------

            page.goto(url, wait_until="domcontentloaded", timeout=self._page_timeout_ms)

            page.wait_for_function(
                "() => window.__NUXT__ && window.__NUXT__.data",
                timeout=15000,
            )

            nuxt_data = page.evaluate("() => window.__NUXT__")

            # give page time to fire API calls
            page.wait_for_timeout(3000)

            records = self._build_records_from_nuxt(nuxt_data)

            # ---------------------------------------------------
            # attach API market data
            # ---------------------------------------------------

            for record in records:

                match_id = record.get("match_id")

                if not match_id:
                    continue

                api_body = api_cache.get(str(match_id))

                if not api_body:
                    continue

                markets = self._parse_match_odds_response(api_body)

                rows = []

                for _, market in markets.items():

                    if not market:
                        continue

                    if isinstance(market, list):
                        rows.extend(market)
                    else:
                        rows.append(market)

                record["market_rows"] = rows

            browser.close()

        return records


    # ============================================================
    # PARSE MATCH ODDS API RESPONSE
    # ============================================================

    def _parse_match_odds_response(self, body: Dict[str, Any]) -> Dict[str, Any]:

        data = body.get("data")

        if not isinstance(data, dict):
            return {}

        periods = data.get("periods") or []
        odds_by_period = data.get("odds") or {}

        market_slug = (
            (data.get("market_name") or "market")
            .lower()
            .replace(" ", "_")
        )

        markets: Dict[str, Any] = {}

        for period in periods:

            pname = period.get("name")
            pid = str(period.get("id"))

            if not pname or not pid:
                continue

            period_data = odds_by_period.get(pid)

            if not isinstance(period_data, dict):
                continue

            inner = period_data.get("odds") or {}

            o1 = inner.get("o1") or {}
            o2 = inner.get("o2") or {}

            home_dec = _parse_float(o1.get("odds_value"))
            away_dec = _parse_float(o2.get("odds_value"))

            if pname.lower() == "final":
                key = market_slug
            else:
                key = f"{market_slug}_{pname.lower().replace(' ','_')}"

            markets[key] = {
                "bookie": o1.get("bookie_name"),
                "bookie_slug": o1.get("bookie_slug"),
                "home_odds_decimal": home_dec,
                "away_odds_decimal": away_dec,
                "home_odds_american": _decimal_to_american(home_dec),
                "away_odds_american": _decimal_to_american(away_dec),
                "status": o1.get("odds_status"),
                "bet_link": o1.get("odds_link"),
                "winning_side": period_data.get("winning_odd"),
            }

        return markets


    # ============================================================
    # NUXT PARSING
    # ============================================================

    def _build_records_from_nuxt(self, nuxt_data: Dict[str, Any]) -> List[Dict[str, Any]]:

        data0 = (nuxt_data.get("data") or [{}])[0]

        raw = {
            "matchList": data0.get("matchList", []),
            "odds": data0.get("odds", {}),
            "sport": (data0.get("currentSport") or {}).get("slug"),
        }

        return self._build_records(raw)


    def _build_records(self, raw: Dict[str, Any]) -> List[Dict[str, Any]]:

        match_list = raw.get("matchList", [])
        odds_by_match = raw.get("odds", {}).get("matches", {})
        sport = raw.get("sport")

        records: List[Dict[str, Any]] = []

        for match in match_list:

            match_id = match.get("id")
            match_odds = odds_by_match.get(str(match_id), {})

            record = {
                "match_id": match_id,
                "sport": sport,
                "date_utc": _normalise_ts(match.get("md")),
                "home_team": match.get("ht"),
                "away_team": match.get("at"),
                "home_team_id": match.get("ht_id"),
                "away_team_id": match.get("at_id"),
                "inplay": match.get("inplay", False),
                "league_id": match.get("league_id"),
                "markets": {},
            }

            for market_id, market_data in match_odds.items():

                market_name = MARKET_NAMES.get(
                    market_id,
                    f"market_{market_id}"
                )

                odds_entries = market_data.get("odds", {})

                home_entry = odds_entries.get("1", {})
                away_entry = odds_entries.get("2", {})

                record["markets"][market_name] = {
                    "home_odds_decimal": _parse_float(home_entry.get("value")),
                    "away_odds_decimal": _parse_float(away_entry.get("value")),
                }

            records.append(record)

        return records


# ============================================================
# UTILITY FUNCTIONS
# ============================================================


def _normalise_ts(value: Optional[str]) -> Optional[str]:

    if not value:
        return value

    return value.split("+")[0].rstrip()


def _parse_float(value: Any) -> Optional[float]:

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _decimal_to_american(decimal_odds: Optional[float]) -> Optional[int]:

    if decimal_odds is None or decimal_odds <= 1:
        return None

    if decimal_odds >= 2:
        return round((decimal_odds - 1) * 100)

    else:
        return round(-100 / (decimal_odds - 1))

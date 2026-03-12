"""
Oddspedia odds scraper.

Fetches the Oddspedia odds page for a given sport/URL and returns a clean list
of matches with their odds.

Live URL scraping uses Playwright (headless Chromium) to bypass bot-detection
and reads window.__NUXT__ directly from the browser context.  After the page
loads, additional per-set market odds are fetched by calling the Oddspedia API
from within the browser context (so Cloudflare cookies are already present).

File-based scraping (for local testing) evaluates the embedded Nuxt SSR state
via a Node.js subprocess.  Per-set odds are not available in file mode since
they require live API calls.

Requirements:
  Live scraping : playwright Python package + `playwright install chromium`
  File scraping : Node.js >=14

Usage:
    from oddspedia_client import OddspediaClient

    client = OddspediaClient()

    # From a live URL (uses Playwright) — includes per-set odds
    matches = client.scrape("https://www.oddspedia.com/us/tennis/odds")

    # From a saved HTML file (uses Node.js, no browser needed)
    matches = client.scrape_file("website_responses/oddspedia/test")
"""

from __future__ import annotations

import json
import logging
import subprocess
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

LOGGER = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Market ID → human-readable name mapping
# ---------------------------------------------------------------------------
# IDs 201/301 come from window.__NUXT__ (SSR-embedded, always present).
# IDs 204/205/304/305/407/408 are fetched live via the API after page load.
MARKET_NAMES: Dict[str, str] = {
    "201": "moneyline",
    "204": "set1_moneyline",
    "205": "set2_moneyline",
    "301": "spread",
    "304": "set1_spread",
    "305": "set2_spread",
    "401": "total",
    "407": "set1_total",
    "408": "set2_total",
}

# Per-set market IDs to fetch via the API after Playwright has loaded the page.
# These are not included in the SSR Nuxt state — the browser fetches them
# on-demand when the user clicks a tab.
_SET_MARKET_IDS: List[int] = [204, 205, 304, 305, 407, 408]

# Base URL for the Oddspedia odds API
_API_BASE = "https://oddspedia.com/api/v1/getAmericanMaxOddsWithPagination"

# Node.js snippet that evaluates the __NUXT__ IIFE and prints the JSON we need.
# Used only by scrape_file(); live scraping reads __NUXT__ directly via Playwright.
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
    """Scrapes odds from Oddspedia pages."""

    def __init__(
        self,
        user_agent: str = (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        page_timeout_ms: int = 60_000,
    ) -> None:
        self._user_agent = user_agent
        self._page_timeout_ms = page_timeout_ms

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def scrape(self, url: str) -> List[Dict[str, Any]]:
        """Fetch *url* using Playwright and return a list of match-odds dicts.

        Uses headless Chromium with playwright-stealth to mask automation
        signals (navigator.webdriver, etc.) that trigger Cloudflare blocks.

        After the page loads, per-set market odds (1st set moneyline/spread/
        total, 2nd set moneyline/spread/total) are fetched by calling the
        Oddspedia API via fetch() from within the browser context — this reuses
        the Cloudflare cookies already established for the page load.

        Waits only for DOMContentLoaded because window.__NUXT__ is embedded
        in the SSR HTML.  "networkidle" is intentionally avoided: Cloudflare's
        challenge scripts keep the network busy indefinitely.
        """
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
            if _stealth_sync is not None:
                _stealth_sync(page)
            page.goto(url, wait_until="domcontentloaded", timeout=self._page_timeout_ms)
            # __NUXT__ is injected by SSR into the initial HTML, so it's
            # available immediately after the DOM is parsed.
            page.wait_for_function("() => !!window.__NUXT__", timeout=15_000)
            nuxt_data = page.evaluate("() => window.__NUXT__")

            # Fetch per-set markets from the API while the browser session is
            # still open so we inherit Cloudflare auth cookies.
            extra_odds = self._fetch_set_markets_via_browser(page)

            browser.close()

        return self._build_records_from_nuxt(nuxt_data, extra_odds=extra_odds)

    def _fetch_set_markets_via_browser(self, page: Any) -> Dict[str, Dict[str, Any]]:
        """Call the Oddspedia API for per-set markets from within the live browser context.

        Because the browser has already passed Cloudflare's challenge for the
        page load, fetch() calls made from the same context inherit those
        cookies automatically.

        Returns a dict of {match_id_str: {market_id_str: market_data}} that can
        be merged into the records built from __NUXT__.
        """
        now = datetime.now(timezone.utc)
        # Oddspedia's date window starts at 04:00 UTC (= midnight US Eastern DST)
        window_start = now.replace(hour=4, minute=0, second=0, microsecond=0)
        if now.hour < 4:
            window_start -= timedelta(days=1)
        window_end = window_start + timedelta(hours=23, minutes=59, seconds=59)

        start_date = window_start.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_date   = window_end.strftime("%Y-%m-%dT%H:%M:%SZ")

        combined: Dict[str, Dict[str, Any]] = {}

        for ot in _SET_MARKET_IDS:
            api_url = (
                f"{_API_BASE}"
                f"?geoCode=US&bookmakerGeoCode=US&bookmakerGeoState=VA&wettsteuer=0"
                f"&startDate={start_date}&endDate={end_date}"
                f"&sport=tennis&ot={ot}&excludeSpecialStatus=0&popularLeaguesOnly=0"
                f"&sortBy=default&status=all&page=1&perPage=50&r=si&inplay=1&language=us"
            )
            LOGGER.info("Fetching set market ot=%s via browser fetch()", ot)
            try:
                result = page.evaluate(
                    """async (url) => {
                        const resp = await fetch(url, {
                            headers: { "Accept": "application/json", "Accept-Language": "en-US,en;q=0.9" }
                        });
                        if (!resp.ok) return null;
                        return await resp.json();
                    }""",
                    api_url,
                )
            except Exception as exc:
                LOGGER.warning("set market ot=%s fetch failed: %s", ot, exc)
                continue

            if not result or "data" not in result:
                LOGGER.warning("set market ot=%s returned no data", ot)
                continue

            for match_id, match_data in result["data"].get("matches", {}).items():
                if match_id not in combined:
                    combined[match_id] = {}
                combined[match_id][str(ot)] = match_data

        return combined

    def scrape_file(self, path: str | Path) -> List[Dict[str, Any]]:
        """Parse a saved HTML file via Node.js and return a list of match-odds dicts.

        Does not require Playwright — useful for local testing against saved
        responses without a browser.
        """
        html = Path(path).read_text(encoding="utf-8", errors="replace")
        return self._parse_html_via_node(html)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_records_from_nuxt(
        self,
        nuxt_data: Dict[str, Any],
        extra_odds: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> List[Dict[str, Any]]:
        """Build records from an already-resolved __NUXT__ dict (Playwright path)."""
        data0 = (nuxt_data.get("data") or [{}])[0]
        raw = {
            "matchList": data0.get("matchList", []),
            "odds":      data0.get("odds", {}),
            "sport":     (data0.get("currentSport") or {}).get("slug"),
        }
        return self._build_records(raw, extra_odds=extra_odds)

    def _parse_html_via_node(self, html: str) -> List[Dict[str, Any]]:
        """Write html to a temp file, evaluate via Node.js, return clean records."""
        with tempfile.NamedTemporaryFile(
            suffix=".html", mode="w", encoding="utf-8", delete=False
        ) as tmp:
            tmp.write(html)
            tmp_path = tmp.name

        try:
            raw = self._evaluate_nuxt_via_node(tmp_path)
        finally:
            Path(tmp_path).unlink(missing_ok=True)

        return self._build_records(raw, extra_odds=None)

    def _evaluate_nuxt_via_node(self, html_path: str) -> Dict[str, Any]:
        """Run the Node.js extractor against *html_path* and return parsed JSON."""
        with tempfile.NamedTemporaryFile(
            suffix=".js", mode="w", encoding="utf-8", delete=False
        ) as js_tmp:
            js_tmp.write(_NODE_EXTRACTOR)
            js_path = js_tmp.name

        try:
            result = subprocess.run(
                ["node", js_path, html_path],
                capture_output=True,
                text=True,
                timeout=30,
            )
        finally:
            Path(js_path).unlink(missing_ok=True)

        if result.returncode != 0:
            raise RuntimeError(
                f"Node.js extractor failed (exit {result.returncode}): {result.stderr.strip()}"
            )

        return json.loads(result.stdout)

    def _build_records(
        self,
        raw: Dict[str, Any],
        extra_odds: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> List[Dict[str, Any]]:
        """Combine matchList, NUXT-embedded odds, and extra API odds into records.

        Parameters
        ----------
        raw:
            Dict with ``matchList``, ``odds``, and ``sport`` keys — parsed from
            window.__NUXT__ (or the Node.js extractor for file mode).
        extra_odds:
            Optional ``{match_id_str: {market_id_str: market_data}}`` dict with
            per-set odds fetched directly from the API (Playwright mode only).
        """
        match_list: List[Dict[str, Any]] = raw.get("matchList", [])
        odds_by_match: Dict[str, Any] = raw.get("odds", {}).get("matches", {})
        sport: Optional[str] = raw.get("sport")

        records: List[Dict[str, Any]] = []
        for match in match_list:
            match_id = match.get("id")
            match_odds = odds_by_match.get(str(match_id), {})

            record: Dict[str, Any] = {
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

            # Merge extra per-set odds (fetched via API) into this match's odds
            if extra_odds and str(match_id) in extra_odds:
                match_odds = {**match_odds, **extra_odds[str(match_id)]}

            for market_id, market_data in match_odds.items():
                market_name = MARKET_NAMES.get(market_id, f"market_{market_id}")
                odds_entries = market_data.get("odds", {})
                winning_odd = market_data.get("winning_odd")

                home_entry = odds_entries.get("1", {})
                away_entry = odds_entries.get("2", {})

                market_record: Dict[str, Any] = {
                    "bookie": home_entry.get("bookie"),
                    "bookie_slug": home_entry.get("slug"),
                    "home_odds": _parse_float(home_entry.get("value")),
                    "away_odds": _parse_float(away_entry.get("value")),
                    "home_odds_decimal": _parse_float(home_entry.get("value")),
                    "away_odds_decimal": _parse_float(away_entry.get("value")),
                    "home_odds_american": _decimal_to_american(
                        _parse_float(home_entry.get("value"))
                    ),
                    "away_odds_american": _decimal_to_american(
                        _parse_float(away_entry.get("value"))
                    ),
                    "status": home_entry.get("status"),
                    "bet_link": home_entry.get("link"),
                    "winning_side": winning_odd,
                }

                # Spread-specific fields
                if home_entry.get("handicap_name") is not None:
                    market_record["home_handicap"] = home_entry.get("handicap_name")
                    market_record["away_handicap"] = away_entry.get("handicap_name")
                    market_record["handicap_label"] = home_entry.get("handicap_name_en")

                record["markets"][market_name] = market_record

            records.append(record)

        return records


# ------------------------------------------------------------------
# Utility functions
# ------------------------------------------------------------------

def _normalise_ts(value: Optional[str]) -> Optional[str]:
    """Normalise an Oddspedia timestamp for BigQuery.

    Oddspedia returns e.g. "2026-03-08 18:00:00+00" with a bare two-digit
    offset.  BigQuery's TIMESTAMP parser requires either no offset or the
    full "+HH:MM" form.  Since the column is date_utc we simply strip the
    offset so the value is unambiguously UTC.
    """
    if not value:
        return value
    # Drop everything from the first "+" onward ("… 18:00:00+00" → "… 18:00:00")
    return value.split("+")[0].rstrip()


def _parse_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _decimal_to_american(decimal_odds: Optional[float]) -> Optional[int]:
    """Convert decimal odds to American (moneyline) format."""
    if decimal_odds is None or decimal_odds <= 1.0:
        return None
    if decimal_odds >= 2.0:
        return round((decimal_odds - 1) * 100)
    return round(-100 / (decimal_odds - 1))


# ------------------------------------------------------------------
# CLI entry point (for quick testing)
# ------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Scrape odds from Oddspedia")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--url", help="Live Oddspedia URL to fetch")
    group.add_argument("--file", help="Saved HTML file to parse")
    parser.add_argument("--out", help="Write JSON output to this file")
    args = parser.parse_args()

    client = OddspediaClient()

    if args.file:
        results = client.scrape_file(args.file)
    else:
        results = client.scrape(args.url)

    output = json.dumps(results, indent=2)

    if args.out:
        Path(args.out).write_text(output, encoding="utf-8")
        print(f"Wrote {len(results)} matches to {args.out}", file=sys.stderr)
    else:
        print(output)

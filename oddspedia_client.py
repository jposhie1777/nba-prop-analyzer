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

# Market IDs found in Oddspedia's Nuxt state
MARKET_NAMES: Dict[str, str] = {
    "201": "moneyline",
    "301": "spread",
}

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

    def scrape(
        self,
        url: str,
        *,
        fetch_set_markets: bool = True,
    ) -> List[Dict[str, Any]]:
        """Fetch *url* using Playwright and return a list of match-odds dicts.

        Uses headless Chromium with playwright-stealth to mask automation
        signals (navigator.webdriver, etc.) that trigger Cloudflare blocks.

        Waits only for DOMContentLoaded because window.__NUXT__ is embedded
        in the SSR HTML — no JS execution is needed.  "networkidle" is
        intentionally avoided: Cloudflare's challenge scripts keep the
        network busy indefinitely and cause a 60 s timeout.

        Parameters
        ----------
        fetch_set_markets:
            When True (default), makes ``getMatchOdds`` API calls for every
            match via the same browser context (same TLS fingerprint + cookies)
            and enriches each record's ``markets`` dict with:

            - ``moneyline_1st_set``, ``moneyline_2nd_set``, … (set-period odds)
            - ``spread`` (main handicap line, e.g. "+4.5/-4.5 Games")
            - ``correct_score_0_2``, ``correct_score_2_0``, … (set-score lines)
            - ``total``, ``total_1st_set``, … (Over/Under lines per period)
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

            # __NUXT__ is injected server-side by Nuxt SSR
            page.wait_for_function(
                "() => window.__NUXT__ && window.__NUXT__.data",
                timeout=15000
            )

            nuxt_data = page.evaluate("() => window.__NUXT__")

            records = self._build_records_from_nuxt(nuxt_data)

            if fetch_set_markets and records:
                api_ctx = context.request
                for record in records:
                    match_id = record.get("match_id")
                    if not match_id:
                        continue

                    set_markets = self._fetch_api_markets(api_ctx, match_id)

                    print(
                        "API MARKET GROUPS:",
                        match_id,
                        type(set_markets),
                        len(set_markets) if isinstance(set_markets, list) else "not-list"
                    )

                    if isinstance(set_markets, list) and set_markets:
                        print("FIRST GROUP KEYS:", set_markets[0].keys())

                    rows = []

                    for market_name, market in set_markets.items():
                        if not market:
                            continue

                        if isinstance(market, list):
                            rows.extend(market)
                        else:
                            rows.append(market)

                    print("PARSED MARKETS:", match_id, len(set_markets))
                    print("ROWS EXPANDED:", match_id, len(rows))

                    record["market_rows"] = rows

            browser.close()

        return records

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

    def _fetch_api_markets(self, api_ctx: Any, match_id: int) -> Dict[str, Any]:
        """Fetch Moneyline (set periods), Spread, Correct Score, and Total via getMatchOdds.

        Makes four separate API calls — one per market type — using the same
        Playwright APIRequestContext (browser TLS fingerprint + session cookies).

        Returns a merged dict of all parsed market records:
        - Moneyline set periods: ``moneyline_1st_set``, ``moneyline_2nd_set``, …
        - Spread main line:      ``spread``
        - Correct score lines:   ``correct_score_2_0``, ``correct_score_0_2``, …
        - Total lines:           ``total``, ``total_1st_set``, …
        """
        markets: Dict[str, Any] = {}

        MARKET_TYPES = [
            None,   # default moneyline endpoint
            201,    # match winner alt endpoint

            301,    # spread
            401,    # total
            800,    # correct score

            202,    # set handicap
            203,    # set total

            601,    # game handicap
            602,    # game total
        ]

        for ot in MARKET_TYPES:

            body = self._call_match_odds_api(api_ctx, match_id, ot=ot)
            print("RAW API KEYS:", match_id, body.keys() if body else None)

            if not body:
                continue

            skip_final = ot is None

            parsed = self._parse_match_odds_response(body, skip_final=skip_final)

            for k, v in parsed.items():
                if k not in markets:
                    markets[k] = v

        return markets

    def _call_match_odds_api(self, api_ctx, match_id: int, *, ot=None):

      qs = (
          f"matchId={match_id}&language=us&geoCode=US"
          "&bookmakerGeoCode=US&bookmakerGeoState=VA"
      )
  
      if ot is not None:
          qs += f"&ot={ot}"
  
      url = f"https://www.oddspedia.com/api/v1/getMatchOdds?{qs}"
  
      try:
  
          resp = api_ctx.get(
              url,
              headers={
                  "accept": "application/json, text/plain, */*",
                  "accept-language": "en-US,en;q=0.9",
                  "origin": "https://www.oddspedia.com",
                  "referer": "https://www.oddspedia.com/us/tennis/odds",
                  "x-requested-with": "XMLHttpRequest",
                  "sec-fetch-site": "same-origin",
                  "sec-fetch-mode": "cors",
                  "sec-fetch-dest": "empty",
                  "user-agent": self._user_agent
              },
              timeout=15000
          )
  
          print("API STATUS:", resp.status)
  
          if resp.status != 200:
              return {}
  
          return resp.json()
  
      except Exception as e:
          print("API ERROR:", e)
          return {}

    def _parse_match_odds_response(
        self,
        body: Dict[str, Any],
        *,
        skip_final: bool = False,
    ) -> Dict[str, Any]:
        """Parse any ``getMatchOdds`` response into market records.

        Handles three response shapes returned by the endpoint:

        **Moneyline** (``waytype=2``, no handicap) — each period entry has a
        direct ``odds: {o1, o2}`` structure::

            "204": {"winning_odd": null, "odds": {"o1": {...}, "o2": {...}}}

        **Spread** (``waytype=2``, ``has_handicap=1``) — each period entry has
        a ``main`` object (primary line) plus an ``alternative`` list::

            "301": {
              "main": {"name": "+4.5/-4.5 Games", "name_en": "+4.5 Games",
                       "odds": {"o1": {...}, "o2": {...}}},
              "alternative": [...]
            }

        **Correct Score** (``waytype=1``) — each period entry has only an
        ``alternative`` list (``main`` is null), with one ``o1`` outcome per
        score line::

            "800": {
              "main": null,
              "alternative": [
                {"name": "0 : 2", "name_en": "0 : 2", "odds": {"o1": {...}}},
                ...
              ]
            }

        Parameters
        ----------
        skip_final:
            When True, periods named "Final" are skipped.  Used for Moneyline
            calls so we don't overwrite the richer SSR-derived Final odds.
        """
        data = body.get("data") if isinstance(body, dict) else None
        if not isinstance(data, dict):
            return {}

        periods: List[Dict[str, Any]] = data.get("periods") or []
        odds_by_period: Dict[str, Any] = data.get("odds") or {}
        market_slug = (data.get("market_name") or "market").lower().replace(" ", "_")

        markets: Dict[str, Any] = {}
        for period in periods:
            pname: str = period.get("name") or ""
            pid: str = str(period.get("id", ""))
            if not pname or not pid:
                continue
            if skip_final and pname.lower() == "final":
                continue

            period_data = odds_by_period.get(pid)
            if not isinstance(period_data, dict):
                continue

            # ----------------------------------------------------------
            # Spread / Total / Correct Score: main + alternative structure
            # ----------------------------------------------------------
            if "main" in period_data or "alternative" in period_data:
                main = period_data.get("main")
                alternatives: List[Dict[str, Any]] = period_data.get("alternative") or []

                # Period suffix: non-Final periods get appended to the key so
                # e.g. Total Final → "total", Total 1st Set → "total_1st_set".
                period_suffix = (
                    ""
                    if pname.lower() == "final"
                    else f"_{pname.lower().replace(' ', '_')}"
                )
                base_key = f"{market_slug}{period_suffix}"

                # Spread / Total — parse the primary (main) line
                if isinstance(main, dict) and main.get("odds"):
                    inner = main.get("odds") or {}
                    o1 = inner.get("o1") or {}
                    o2 = inner.get("o2") or {}
                    home_dec = _parse_float(o1.get("odds_value"))
                    away_dec = _parse_float(o2.get("odds_value"))
                    # "+4.5/-4.5 Games" → home="+4.5 Games", away="-4.5 Games"
                    # "19.5 Games" (total) → no "/" → home_hcp = full name
                    hcp_full = main.get("name") or ""
                    parts = hcp_full.split("/")
                    home_hcp = parts[0].strip() if parts else None
                    away_hcp = parts[1].strip() if len(parts) > 1 else None
                    markets[base_key] = {
                        "bookie":             o1.get("bookie_name"),
                        "bookie_slug":        o1.get("bookie_slug"),
                        "home_odds_decimal":  home_dec,
                        "away_odds_decimal":  away_dec,
                        "home_odds_american": _decimal_to_american(home_dec),
                        "away_odds_american": _decimal_to_american(away_dec),
                        "home_handicap":      home_hcp,
                        "away_handicap":      away_hcp,
                        "handicap_label":     hcp_full,
                        "status":             o1.get("odds_status"),
                        "bet_link":           o1.get("odds_link"),
                        "winning_side":       main.get("winning_odd"),
                    }

                # Correct Score + alternative Spread/Total lines
                for alt in alternatives:
                    alt_label = (alt.get("name_en") or alt.get("name") or "").strip()
                    inner = alt.get("odds") or {}
                    o1 = inner.get("o1") or {}
                    o2 = inner.get("o2") or {}
                    if not o1 or not alt_label:
                        continue
                    home_dec = _parse_float(o1.get("odds_value"))
                    away_dec = _parse_float(o2.get("odds_value"))  # None for CS
                    # Spread/Total alt: derive handicap labels from full name
                    # "-1.5/+1.5 Sets" → home="-1.5 Sets", away="+1.5 Sets"
                    # Correct Score: home_handicap = score label ("0 : 2")
                    full_name = (alt.get("name") or "").strip()
                    hcp_parts = full_name.split("/")
                    home_hcp = hcp_parts[0].strip() if hcp_parts else alt_label
                    away_hcp = hcp_parts[1].strip() if len(hcp_parts) > 1 else None
                    market_key = f"{base_key}_{_safe_key_suffix(alt_label)}"
                    markets[market_key] = {
                        "bookie":             o1.get("bookie_name"),
                        "bookie_slug":        o1.get("bookie_slug"),
                        "home_odds_decimal":  home_dec,
                        "away_odds_decimal":  away_dec,
                        "home_odds_american": _decimal_to_american(home_dec),
                        "away_odds_american": _decimal_to_american(away_dec),
                        "home_handicap":      home_hcp,
                        "away_handicap":      away_hcp,
                        "handicap_label":     full_name or None,
                        "status":             o1.get("odds_status"),
                        "bet_link":           o1.get("odds_link"),
                        "winning_side":       alt.get("winning_odd"),
                    }

            # ----------------------------------------------------------
            # Moneyline: direct odds.{o1, o2} structure
            # ----------------------------------------------------------
            else:
                inner = period_data.get("odds") or {}
                o1 = inner.get("o1") or {}
                o2 = inner.get("o2") or {}
                home_dec = _parse_float(o1.get("odds_value"))
                away_dec = _parse_float(o2.get("odds_value"))
                # "Final" → "moneyline";  "1st Set" → "moneyline_1st_set"
                if pname.lower() == "final":
                    market_key = market_slug
                else:
                    market_key = f"{market_slug}_{pname.lower().replace(' ', '_')}"
                markets[market_key] = {
                    "bookie":             o1.get("bookie_name"),
                    "bookie_slug":        o1.get("bookie_slug"),
                    "home_odds_decimal":  home_dec,
                    "away_odds_decimal":  away_dec,
                    "home_odds_american": _decimal_to_american(home_dec),
                    "away_odds_american": _decimal_to_american(away_dec),
                    "status":             o1.get("odds_status"),
                    "bet_link":           o1.get("odds_link"),
                    "winning_side":       period_data.get("winning_odd"),
                }

        return markets

    def _build_records_from_nuxt(self, nuxt_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Build records from an already-resolved __NUXT__ dict (Playwright path)."""
        data0 = (nuxt_data.get("data") or [{}])[0]
        raw = {
            "matchList": data0.get("matchList", []),
            "odds":      data0.get("odds", {}),
            "sport":     (data0.get("currentSport") or {}).get("slug"),
        }
        return self._build_records(raw)

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

        return self._build_records(raw)

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

    def _build_records(self, raw: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Combine matchList and odds into clean per-match records."""
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

def _safe_key_suffix(text: str) -> str:
    """Convert an outcome label into a safe market-key suffix.

    Examples::

        "0 : 2"        → "0_2"
        "2 : 1"        → "2_1"
        "+4.5 Games"   → "p4_5_games"
        "-1.5 Sets"    → "m1_5_sets"
    """
    return (
        text.lower()
        .replace(" : ", "_")
        .replace("+", "p")
        .replace("-", "m")
        .replace(".", "_")
        .replace("/", "_")
        .replace(" ", "_")
        .strip("_")
    )


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

    import datetime

    parser = argparse.ArgumentParser(description="Scrape odds from Oddspedia")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--url", help="Live Oddspedia URL to fetch")
    group.add_argument("--file", help="Saved HTML file to parse")
    parser.add_argument("--out", help="Write JSON output to this file")
    parser.add_argument(
        "--today",
        action="store_true",
        help="Only include matches scheduled for today (UTC)",
    )
    parser.add_argument(
        "--has-total",
        action="store_true",
        help="Only include matches that have a 'total' market",
    )
    args = parser.parse_args()

    client = OddspediaClient()

    if args.file:
        results = client.scrape_file(args.file)
    else:
        results = client.scrape(args.url)

    if args.today:
        today = datetime.datetime.utcnow().strftime("%Y-%m-%d")
        results = [r for r in results if (r.get("date_utc") or "").startswith(today)]

    if args.has_total:
        results = [r for r in results if "total" in r.get("markets", {})]

    output = json.dumps(results, indent=2)

    if args.out:
        Path(args.out).write_text(output, encoding="utf-8")
        print(f"Wrote {len(results)} matches to {args.out}", file=sys.stderr)
    else:
        print(output)

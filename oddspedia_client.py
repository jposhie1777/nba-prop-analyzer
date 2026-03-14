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
    "204": "moneyline_1st_set",
    "205": "moneyline_2nd_set",
    "301": "handicap",
    "401": "total_sets",
    "501": "market_501",
    "502": "market_502",
    "503": "market_503",
    "601": "market_601",
    "602": "market_602",
}

_MATCH_ODDS_URL = "https://www.oddspedia.com/api/v1/getMatchOdds"
_MATCH_ODDS_PARAMS = "language=us&geoCode=US&bookmakerGeoCode=US&bookmakerGeoState=VA"
_API_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.oddspedia.com/us/tennis/odds",
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

            # ── Load listing page (default view) ─────────────────────────────────
            # The server-side-rendered __NUXT__ already contains moneyline (201),
            # handicap (301), and total_sets (401) for all matches.
            # Client-side API calls are blocked by Cloudflare WAF in CI, so we
            # rely entirely on SSR data.

            print(f"[scraper] Loading default listing page: {url}")
            page.goto(url, wait_until="domcontentloaded", timeout=self._page_timeout_ms)
            page.wait_for_function(
                "() => window.__NUXT__ && window.__NUXT__.data",
                timeout=15000,
            )
            nuxt_data = page.evaluate("() => window.__NUXT__")

            records = self._build_records_from_nuxt(nuxt_data)
            print(f"[scraper] Default page: {len(records)} matches")
            if records:
                m0 = records[0]
                print(f"[scraper]   First match markets: {list(m0.get('markets', {}).keys())}")

            # ── Load listing page with extra ot= values ────────────────────────
            # Oddspedia's listing page accepts an ?ot= query parameter that
            # changes which market the server-side rendering populates in __NUXT__.
            # We try several candidate values to discover 1st/2nd set winner and
            # any other alternate markets.
            #
            # Known ot= values (from getAmericanMaxOddsWithPagination sweeps):
            #   201  moneyline / match winner   ← default
            #   301  handicap
            #   401  total sets O/U
            # Candidates for set-specific markets (to be confirmed by log output):
            #   204  possibly 1st Set winner
            #   205  possibly 2nd Set winner
            #   501, 502, 503, 601 … unknown

            EXTRA_OT_CANDIDATES = [204, 205, 501, 502, 503, 601, 602]

            records_by_id = {str(r["match_id"]): r for r in records if r.get("match_id")}

            for ot_val in EXTRA_OT_CANDIDATES:
                sep = "&" if "?" in url else "?"
                extra_url = f"{url}{sep}ot={ot_val}"
                try:
                    page.goto(extra_url, wait_until="domcontentloaded", timeout=30000)
                    # Wait for any __NUXT__ to appear — listing pages may not populate .data
                    # for unknown ot= values, so we don't require .data here.
                    try:
                        page.wait_for_function("() => !!window.__NUXT__", timeout=10000)
                    except Exception:
                        pass  # evaluate anyway; log what we got

                    extra_nuxt = page.evaluate("() => window.__NUXT__ || null")
                    if not extra_nuxt:
                        print(f"[scraper] ot={ot_val}: no __NUXT__ on page")
                        continue

                    # Log the top-level NUXT keys and data[0] keys so we can
                    # understand the structure for unknown ot= values.
                    nuxt_keys = list(extra_nuxt.keys())
                    data0_x = (extra_nuxt.get("data") or [{}])[0]
                    data0_keys = list(data0_x.keys()) if isinstance(data0_x, dict) else []
                    print(
                        f"[scraper] ot={ot_val}: __NUXT__ keys={nuxt_keys}, "
                        f"data[0] keys={data0_keys[:15]}"
                    )

                    extra_odds = self._extract_odds_from_nuxt(extra_nuxt)

                    new_market_ids: set = set()
                    for mid_str, market_map in extra_odds.items():
                        for mkt_id in market_map:
                            new_market_ids.add(str(mkt_id))

                    print(
                        f"[scraper] ot={ot_val}: {len(extra_odds)} matches, "
                        f"market IDs seen: {sorted(new_market_ids)}"
                    )

                    # Merge any genuinely new market IDs into existing records
                    merged = 0
                    for mid_str, market_map in extra_odds.items():
                        rec = records_by_id.get(mid_str)
                        if not rec:
                            continue
                        for mkt_id, mkt_data in market_map.items():
                            mkt_name = MARKET_NAMES.get(str(mkt_id), f"market_{mkt_id}")
                            if mkt_name not in rec["markets"]:
                                rec["markets"][mkt_name] = self._listing_market_entry(
                                    mkt_name, mkt_id, mkt_data
                                )
                                merged += 1
                    if merged:
                        print(f"[scraper]   → merged {merged} new market entries from ot={ot_val}")

                except Exception as exc:
                    print(f"[scraper] Extra load ot={ot_val} failed: {exc}")

            # ── Navigate to first match page; read its __NUXT__ ───────────────
            # The match page's SSR __NUXT__ contains the full per-match market
            # data (all market groups, all periods).  Log its structure so we
            # can implement proper per-match extraction in a follow-up commit.

            first_match_url = self._first_match_url(nuxt_data)
            print(f"[scraper] First match URL: {first_match_url}")

            if first_match_url:
                try:
                    page.goto(
                        first_match_url,
                        wait_until="domcontentloaded",
                        timeout=30000,
                    )
                    # Match pages use a different NUXT layout — just wait for
                    # any __NUXT__ to exist, don't require .data specifically.
                    try:
                        page.wait_for_function("() => !!window.__NUXT__", timeout=15000)
                    except Exception:
                        pass

                    match_nuxt = page.evaluate("() => window.__NUXT__ || null")
                    if not match_nuxt:
                        print("[scraper] Match page: no __NUXT__ found")
                    else:
                        print(f"[scraper] Match page __NUXT__ top keys: {list(match_nuxt.keys())}")
                        # Drill into every top-level key to find market/odds data
                        for tk, tv in match_nuxt.items():
                            if isinstance(tv, dict):
                                print(f"[scraper]   .{tk} (dict) keys: {list(tv.keys())[:15]}")
                                # One more level for promising keys
                                for sk in ("data", "odds", "match", "markets", "matchOdds"):
                                    sv = tv.get(sk)
                                    if sv is not None:
                                        if isinstance(sv, (dict, list)):
                                            inner_keys = list(sv.keys())[:10] if isinstance(sv, dict) else f"list[{len(sv)}]"
                                            print(f"[scraper]     .{tk}.{sk}: {inner_keys}")
                            elif isinstance(tv, list):
                                print(f"[scraper]   .{tk} (list[{len(tv)}])")
                                if tv and isinstance(tv[0], dict):
                                    print(f"[scraper]     [0] keys: {list(tv[0].keys())[:15]}")
                except Exception as exc:
                    print(f"[scraper] Match page load failed: {exc}")

            # ── Summary ───────────────────────────────────────────────────────
            total_markets = sum(len(r.get("markets", {})) for r in records)
            print(
                f"[scraper] Final: {len(records)} matches, "
                f"{total_markets} total market entries "
                f"(avg {total_markets/max(len(records),1):.1f}/match)"
            )
            if records:
                all_mkt_names = sorted({k for r in records for k in r.get("markets", {}).keys()})
                print(f"[scraper] All market names seen: {all_mkt_names}")

            browser.close()

        return records


    # ============================================================
    # HELPERS
    # ============================================================

    def _extract_odds_from_nuxt(self, nuxt_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract {match_id: {market_id: market_data}} from any NUXT listing page."""
        try:
            data0 = (nuxt_data.get("data") or [{}])[0]
            return (data0.get("odds") or {}).get("matches") or {}
        except Exception:
            return {}

    def _listing_market_entry(
        self, market_name: str, market_id: Any, market_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Build a market dict from listing API (getAmericanMaxOddsWithPagination) data."""
        odds_entries = market_data.get("odds", {}) if isinstance(market_data, dict) else {}
        home_entry = odds_entries.get("1", {})
        away_entry = odds_entries.get("2", {})
        home_dec = _parse_float(home_entry.get("value"))
        away_dec = _parse_float(away_entry.get("value"))
        return {
            "market": market_name,
            "period_id": _safe_int_local(market_id),
            "home_odds_decimal": home_dec,
            "away_odds_decimal": away_dec,
            "home_odds_american": _decimal_to_american(home_dec),
            "away_odds_american": _decimal_to_american(away_dec),
            "bookie_id": home_entry.get("bid"),
            "bookie": home_entry.get("bookie"),
            "bookie_slug": home_entry.get("slug"),
            "bet_link": home_entry.get("link"),
            "odds_status": home_entry.get("status"),
            "home_handicap": home_entry.get("handicap_name_en") or home_entry.get("handicap_name"),
            "away_handicap": away_entry.get("handicap_name_en") or away_entry.get("handicap_name"),
            "handicap_label": home_entry.get("handicap_name_en"),
            "winning_side": market_data.get("winning_odd"),
        }

    def _first_match_url(self, nuxt_data: Dict[str, Any]) -> Optional[str]:
        """Return the URL of the first upcoming match from the NUXT matchList."""
        try:
            data0 = (nuxt_data.get("data") or [{}])[0]
            match_list = data0.get("matchList") or []
            for m in match_list:
                ht_slug = m.get("ht_slug") or m.get("htSlug")
                at_slug = m.get("at_slug") or m.get("atSlug")
                mid = m.get("id") or m.get("match_id") or m.get("matchId")
                raw_url = m.get("url")
                if raw_url and isinstance(raw_url, str) and "/tennis/" in raw_url:
                    base = "https://www.oddspedia.com"
                    return raw_url if raw_url.startswith("http") else base + raw_url
                if ht_slug and at_slug:
                    slug = f"{ht_slug}-{at_slug}"
                    if mid:
                        slug += f"-{mid}"
                    return f"https://www.oddspedia.com/us/tennis/{slug}"
        except Exception as exc:
            print(f"[scraper] _first_match_url error: {exc}")
        return None

    # ============================================================
    # PARSE MATCH ODDS API RESPONSE
    # ============================================================

    def _parse_match_odds_response(self, body: Dict[str, Any]) -> Dict[str, Any]:

        data = body.get("data")

        if not isinstance(data, dict):
            return {}

        periods = data.get("periods") or []
        odds_by_period = data.get("odds") or {}
        outcome_names = data.get("outcome_names") or []
        market_group_id = data.get("market_group_id")
        raw_market_name = data.get("market_name") or "market"

        market_slug = raw_market_name.lower().replace(" ", "_")

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

            # Derive a clean market key:
            # "Final" period keeps the parent market slug (e.g. "moneyline"),
            # other periods get it appended (e.g. "moneyline_1st_set").
            if pname.lower() == "final":
                key = market_slug
            else:
                key = f"{market_slug}_{pname.lower().replace(' ', '_')}"

            # Resolve outcome names from the outcome_names array when present
            home_name = (
                outcome_names[0]
                if outcome_names and len(outcome_names) >= 1
                else o1.get("outcome_name", "Home")
            )
            away_name = (
                outcome_names[1]
                if outcome_names and len(outcome_names) >= 2
                else o2.get("outcome_name", "Away")
            )

            markets[key] = {
                # Market / period metadata
                "market": key,
                "market_group_id": market_group_id,
                "market_group_name": raw_market_name,
                "period_id": _safe_int_local(pid),
                "period_name": pname,
                # Bookie info (from the home-side outcome as the representative)
                "bookie_id": o1.get("bid"),
                "bookie": o1.get("bookie_name"),
                "bookie_slug": o1.get("bookie_slug"),
                # 2-way convenience odds
                "home_odds_decimal": home_dec,
                "away_odds_decimal": away_dec,
                "home_odds_american": _decimal_to_american(home_dec),
                "away_odds_american": _decimal_to_american(away_dec),
                # Extra per-outcome metadata
                "home_outcome_name": home_name,
                "away_outcome_name": away_name,
                "odds_status": o1.get("odds_status"),
                "odds_direction": o1.get("odds_direction"),
                "bet_link": o1.get("odds_link"),
                "winning_side": period_data.get("winning_odd"),
                # Raw payload for debugging
                "market_json": data,
                "outcome_json": inner,
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

                home_dec = _parse_float(home_entry.get("value"))
                away_dec = _parse_float(away_entry.get("value"))

                record["markets"][market_name] = {
                    "market": market_name,
                    "period_id": _safe_int_local(market_id),
                    "home_odds_decimal": home_dec,
                    "away_odds_decimal": away_dec,
                    "home_odds_american": _decimal_to_american(home_dec),
                    "away_odds_american": _decimal_to_american(away_dec),
                    "bookie_id": home_entry.get("bid"),
                    "bookie": home_entry.get("bookie"),
                    "bookie_slug": home_entry.get("slug"),
                    "bet_link": home_entry.get("link"),
                    "odds_status": home_entry.get("status"),
                    "home_handicap": home_entry.get("handicap_name_en") or home_entry.get("handicap_name"),
                    "away_handicap": away_entry.get("handicap_name_en") or away_entry.get("handicap_name"),
                    "handicap_label": home_entry.get("handicap_name_en"),
                    "winning_side": market_data.get("winning_odd"),
                }

            records.append(record)

        return records


# ============================================================
# UTILITY FUNCTIONS
# ============================================================


def _safe_int_local(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


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

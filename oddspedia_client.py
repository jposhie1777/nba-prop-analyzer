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

# Per-match API – fetched via the live Playwright session (bypasses Cloudflare WAF)
_PER_MATCH_API = "https://www.oddspedia.com/api/v1/getMatchMaxOddsByGroup"
_PER_MATCH_PARAMS = "inplay=0&geoCode=US&geoState=NY&language=us"
# Market group IDs to fetch for every match (201=Moneyline, 301=Spread, 401=Total Sets)
_PER_MATCH_MARKET_GROUPS = [201, 301, 401]

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

            # ── Per-match odds via getMatchMaxOddsByGroup API ─────────────────
            # Use the live Playwright session (which has valid Cloudflare
            # cookies) to call the per-match API for each match.  This gives
            # all bookmakers and all lines (main + alternative) for every
            # market group — far richer than the single best-odd from the
            # listing-page SSR.

            print(
                f"[scraper] Fetching per-match odds for {len(records)} matches "
                f"× {len(_PER_MATCH_MARKET_GROUPS)} market groups …"
            )
            total_market_rows = 0
            for record in records:
                mid = str(record.get("match_id") or "")
                if not mid:
                    continue
                all_market_rows: List[Dict[str, Any]] = []
                for mg_id in _PER_MATCH_MARKET_GROUPS:
                    body = self._fetch_per_match_api(page, mid, mg_id)
                    if body:
                        mrows = self._parse_per_match_to_market_rows(body, mid)
                        all_market_rows.extend(mrows)
                if all_market_rows:
                    record["market_rows"] = all_market_rows
                    total_market_rows += len(all_market_rows)
            print(
                f"[scraper] Per-match fetch complete: "
                f"{total_market_rows} outcome rows across {len(records)} matches"
            )

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
    # PER-MATCH API FETCHING
    # ============================================================

    def _fetch_per_match_api(
        self, page: Any, match_id: str, market_group_id: int
    ) -> Optional[Dict[str, Any]]:
        """
        Call getMatchMaxOddsByGroup from within the live Playwright browser
        session.  Because the request originates from a real browser context
        with valid Cloudflare cookies it is not blocked by the WAF.
        """
        url = (
            f"{_PER_MATCH_API}"
            f"?matchId={match_id}&marketGroupId={market_group_id}&{_PER_MATCH_PARAMS}"
        )
        try:
            result = page.evaluate(
                """async (url) => {
                    try {
                        const r = await fetch(url, {
                            headers: {'Accept': 'application/json'}
                        });
                        if (!r.ok) return null;
                        return await r.json();
                    } catch (e) {
                        return null;
                    }
                }""",
                url,
            )
            return result if isinstance(result, dict) else None
        except Exception as exc:
            print(
                f"[scraper] per-match API failed "
                f"matchId={match_id} mgId={market_group_id}: {exc}"
            )
            return None

    def _parse_per_match_to_market_rows(
        self, body: Dict[str, Any], match_id: str
    ) -> List[Dict[str, Any]]:
        """
        Convert a getMatchMaxOddsByGroup response body into the market_rows
        format expected by oddspedia_odds_ingest._normalize_rich_market_rows().

        Handles both shapes that the API can return per period:
          • flat   {"odds": {"o1": ..., "o2": ...}, "winning_odd": ...}
          • tiered {"main": {...}, "alternative": [{...}, ...]}
        """
        data = body.get("data") or {}
        periods_list = body.get("periods") or []
        periods_by_id = {
            str(p["id"]): p.get("name")
            for p in periods_list
            if isinstance(p, dict) and "id" in p
        }

        outcome_names: List[str] = data.get("outcome_names") or []
        market_group_id = data.get("market_group_id")
        market_name_full: str = data.get("market_name") or "market"
        market_slug = market_name_full.lower().replace(" ", "_")

        rows: List[Dict[str, Any]] = []

        for period_key, period_obj in (data.get("odds") or {}).items():
            if not isinstance(period_obj, dict):
                continue

            period_id = _safe_int_local(period_key)
            period_name = periods_by_id.get(str(period_key))

            # Determine entry list (main line + any alternative lines)
            if "main" in period_obj:
                entries: List[tuple] = []
                main = period_obj.get("main")
                if isinstance(main, dict):
                    entries.append((None, main))   # main line: line_value=None
                for alt in (period_obj.get("alternative") or []):
                    if isinstance(alt, dict):
                        entries.append(("alternative", alt))
            else:
                entries = [(None, period_obj)]

            for _line_type, entry in entries:
                if not isinstance(entry, dict):
                    continue

                line_value = entry.get("name")   # e.g. "-3.5 Games" or None
                odds_dict = entry.get("odds") or {}

                # Convenience: grab o1/o2 decimals for the whole entry row
                o1_obj = odds_dict.get("o1") or {}
                o2_obj = odds_dict.get("o2") or {}
                home_dec = _parse_float(o1_obj.get("odds_value"))
                away_dec = _parse_float(o2_obj.get("odds_value"))

                for idx, (outcome_key, odd_obj) in enumerate(odds_dict.items(), start=1):
                    if not isinstance(odd_obj, dict):
                        continue

                    # Resolve outcome name from the outcome_names array
                    try:
                        key_num = int(outcome_key.lstrip("o")) - 1
                        outcome_name = (
                            outcome_names[key_num]
                            if 0 <= key_num < len(outcome_names)
                            else outcome_key
                        )
                    except (ValueError, AttributeError):
                        outcome_name = outcome_key

                    # Map o1→home, o2→away, anything else→None
                    if outcome_key == "o1":
                        outcome_side = "home"
                    elif outcome_key == "o2":
                        outcome_side = "away"
                    else:
                        outcome_side = None

                    odds_dec = _parse_float(odd_obj.get("odds_value"))

                    rows.append(
                        {
                            "market_group_id": market_group_id,
                            "market_group_name": market_name_full,
                            "market": market_slug,
                            "period_id": period_id,
                            "period_name": period_name,
                            "bookie_id": odd_obj.get("bid"),
                            "bookie": odd_obj.get("bookie_name"),
                            "bookie_slug": odd_obj.get("bookie_slug"),
                            "outcome_key": outcome_key,
                            "outcome_name": outcome_name,
                            "outcome_side": outcome_side,
                            "outcome_order": idx,
                            "odds_decimal": odds_dec,
                            "odds_american": _decimal_to_american(odds_dec),
                            "odds_status": odd_obj.get("odds_status"),
                            "odds_direction": odd_obj.get("odds_direction"),
                            "line_value": line_value,
                            "home_handicap": None,
                            "away_handicap": None,
                            "handicap_label": None,
                            "winning_side": entry.get("winning_odd"),
                            "bet_link": odd_obj.get("odds_link"),
                            # Convenience columns: both sides' decimals on every row
                            "home_odds_decimal": home_dec,
                            "away_odds_decimal": away_dec,
                            "home_odds_american": _decimal_to_american(home_dec),
                            "away_odds_american": _decimal_to_american(away_dec),
                            "market_json": data,
                            "outcome_json": odd_obj,
                        }
                    )

        return rows

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

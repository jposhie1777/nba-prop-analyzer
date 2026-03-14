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
    "301": "handicap",
    "401": "total_sets",
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

            # ---------------------------------------------------
            # API RESPONSE CACHE
            # ---------------------------------------------------

            from urllib.parse import urlparse, parse_qs

            api_cache: Dict[str, Any] = {}
            all_api_urls: List[str] = []  # debug: every /api/ call the page makes

            def handle_response(resp):
                """Cache getMatchOdds XHR fired by the page.

                Match ID is extracted from the URL query string because the
                response body does NOT include it.
                """
                path = urlparse(resp.url).path
                if "/api/" in path:
                    all_api_urls.append(f"[{resp.status}] {resp.url[:120]}")

                if "getMatchOdds" not in resp.url:
                    return

                qs = parse_qs(urlparse(resp.url).query)
                url_mid = (qs.get("matchId") or qs.get("id") or [None])[0]

                if resp.status != 200:
                    print(f"[scraper] getMatchOdds XHR status={resp.status} mid={url_mid}")
                    return

                try:
                    data = resp.json()
                    body_mid = (data.get("data") or {}).get("match_id")
                    mid = body_mid or url_mid

                    if mid:
                        api_cache[str(mid)] = data
                        print(f"[scraper] XHR cached matchId={mid}")

                except Exception as exc:
                    print(f"[scraper] XHR parse error for {resp.url[:80]}: {exc}")

            page.on("response", handle_response)

            # ---------------------------------------------------

            print(f"[scraper] Navigating to {url}")
            page.goto(url, wait_until="domcontentloaded", timeout=self._page_timeout_ms)
            print("[scraper] domcontentloaded fired")

            # Attempt to expand match rows so the page fires getMatchOdds XHRs
            try:
                n_rows = page.evaluate("""
                    () => {
                        const rows = document.querySelectorAll('[data-match-id]');
                        rows.forEach(r => r.click());
                        return rows.length;
                    }
                """)
                print(f"[scraper] Clicked {n_rows} [data-match-id] rows")
            except Exception as exc:
                print(f"[scraper] Row-click failed: {exc}")

            page.wait_for_function(
                "() => window.__NUXT__ && window.__NUXT__.data",
                timeout=15000,
            )
            print("[scraper] __NUXT__ ready")

            nuxt_data = page.evaluate("() => window.__NUXT__")

            # Give the page a moment to fire any background XHRs from click sim
            page.wait_for_timeout(3000)

            print(f"[scraper] API URLs fired so far ({len(all_api_urls)} total):")
            for u in all_api_urls[:20]:
                print(f"  {u}")
            if len(all_api_urls) > 20:
                print(f"  … (+{len(all_api_urls) - 20} more)")

            records = self._build_records_from_nuxt(nuxt_data)
            print(f"[scraper] Built {len(records)} records from NUXT")

            # Debug: inspect first match structure
            if records:
                m0 = records[0]
                print(f"[scraper] First match: id={m0.get('match_id')} "
                      f"home={m0.get('home_team')} away={m0.get('away_team')}")
                print(f"[scraper] First match markets: {list(m0.get('markets', {}).keys())}")

            # ---------------------------------------------------
            # Try a single diagnostic fetch to understand why in-page
            # fetch might be blocked.
            # ---------------------------------------------------
            if records:
                first_mid = str(records[0].get("match_id", ""))
                if first_mid:
                    diag = page.evaluate(
                        """
                        async (mid) => {
                            const url = `https://www.oddspedia.com/api/v1/getMatchOdds` +
                                `?matchId=${mid}&language=us&geoCode=US` +
                                `&bookmakerGeoCode=US&bookmakerGeoState=VA`;
                            try {
                                const r = await fetch(url, {
                                    headers: { 'Accept': 'application/json' }
                                });
                                let body = '';
                                try { body = await r.text(); } catch(_) {}
                                return {
                                    status: r.status,
                                    ok: r.ok,
                                    bodyPreview: body.substring(0, 300),
                                    url: url
                                };
                            } catch (e) {
                                return { error: String(e), url: url };
                            }
                        }
                        """,
                        first_mid,
                    )
                    print(f"[scraper] DIAG fetch for matchId={first_mid}: {diag}")

            # ---------------------------------------------------
            # Navigate to first match page to get Cloudflare match-page
            # cookies, then retry in-page fetch from that context.
            # ---------------------------------------------------
            match_page_loaded = False
            match_page = context.new_page()
            try:
                # Build first match URL from slugs in the NUXT matchList
                first_match_url = self._first_match_url(nuxt_data)
                print(f"[scraper] First match URL: {first_match_url}")

                if first_match_url:
                    print(f"[scraper] Navigating to match page for cookie/context setup…")
                    match_page.on("response", handle_response)  # capture XHR here too
                    match_page.goto(
                        first_match_url,
                        wait_until="domcontentloaded",
                        timeout=30000,
                    )
                    match_page.wait_for_timeout(4000)  # let getMatchOdds XHR fire
                    match_page_loaded = True
                    print(f"[scraper] Match page loaded; api_cache now has {len(api_cache)} entries")

            except Exception as exc:
                print(f"[scraper] Match page navigation failed: {exc}")
            finally:
                try:
                    match_page.close()
                except Exception:
                    pass

            # ---------------------------------------------------
            # For remaining matches not yet in api_cache, fire
            # in-page fetch from the listing page context.
            # ---------------------------------------------------
            missing_ids = [
                str(r["match_id"])
                for r in records
                if r.get("match_id") and str(r["match_id"]) not in api_cache
            ]

            print(
                f"[scraper] XHR captured {len(api_cache)} matches; "
                f"fetching {len(missing_ids)} via in-page fetch"
            )

            if missing_ids:
                try:
                    fetched = page.evaluate(
                        """
                        async (matchIds) => {
                            const BASE = 'https://www.oddspedia.com/api/v1/getMatchOdds';
                            const PARAMS = 'language=us&geoCode=US&bookmakerGeoCode=US' +
                                           '&bookmakerGeoState=VA';
                            const BATCH = 8;
                            const out = {};
                            const errors = [];
                            for (let i = 0; i < matchIds.length; i += BATCH) {
                                const batch = matchIds.slice(i, i + BATCH);
                                const results = await Promise.allSettled(
                                    batch.map(mid =>
                                        fetch(`${BASE}?matchId=${mid}&${PARAMS}`, {
                                            headers: { 'Accept': 'application/json' }
                                        })
                                        .then(r => {
                                            if (!r.ok) {
                                                errors.push(`${mid}:${r.status}`);
                                                return null;
                                            }
                                            return r.json().then(d => [mid, d]);
                                        })
                                        .catch(e => { errors.push(`${mid}:${String(e)}`); return null; })
                                    )
                                );
                                for (const r of results) {
                                    if (r.status === 'fulfilled' && r.value) {
                                        out[r.value[0]] = r.value[1];
                                    }
                                }
                            }
                            return { data: out, errors: errors.slice(0, 10) };
                        }
                        """,
                        missing_ids,
                    )

                    if isinstance(fetched, dict):
                        errors = fetched.get("errors", [])
                        data = fetched.get("data", {})
                        api_cache.update(data)
                        print(f"[scraper] In-page fetch: got {len(data)} matches, "
                              f"errors={errors[:5]}")

                except Exception as exc:
                    print(f"[scraper] In-page fetch block error: {exc}")

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

                for market_key, market in markets.items():

                    if not market:
                        continue

                    if isinstance(market, list):
                        rows.extend(market)
                    else:
                        rows.append(market)

                record["market_rows"] = rows

            with_market_rows = sum(1 for r in records if r.get("market_rows"))
            print(
                f"[scraper] {with_market_rows}/{len(records)} records have market_rows "
                f"(api_cache size={len(api_cache)})"
            )

            browser.close()

        return records


    # ============================================================
    # HELPERS
    # ============================================================

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

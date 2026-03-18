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
from collections import defaultdict
import logging
import subprocess
import tempfile
from pathlib import Path
from urllib.parse import urlparse
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
_PER_MATCH_API = "https://oddspedia.com/api/v1/getMatchMaxOddsByGroup"
_PER_MATCH_PARAMS = "inplay=0&geoCode=US&geoState=NY&language=us"
# Market group IDs to fetch for every match (201=Moneyline, 301=Spread, 401=Total Sets)
_PER_MATCH_MARKET_GROUPS = [
    1,   # Match Winner (1x2)
    2,   # Handicap
    3,   # Totals
    4,   # Both teams score
    5,   # Double chance
    6,   # Draw no bet
    7,   # Team totals
    8,
    9,
    10,
    63,  # Total Corners
]


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
    
    def scrape(
        self,
        url: str,
        *,
        league_category: str = "usa",
        league_slug: str = "mls",
        season_id: int = 137218,
        sport: str = "soccer",
    ) -> List[Dict[str, Any]]:

        from camoufox.sync_api import Camoufox

        LOGGER.info("Fetching %s via Camoufox", url)

        with Camoufox(headless=True, geoip=True) as browser:

            context = browser.new_context(
                locale="en-US",
                viewport={"width": 1920, "height": 1080},
                extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
            )

            page = context.new_page()

            # ── Load listing page (default view) ─────────────────────────────────
            # For SSR-heavy pages (e.g. tennis), window.__NUXT__.data[0].matchList
            # is populated by the server.  For client-side pages (e.g. MLS soccer),
            # data is [] but the page fires API calls after hydration — we intercept
            # those to discover match IDs.

            # Intercept ALL Oddspedia API responses on the listing page so we can
            # fall back to them when the SSR match list is empty.
            listing_api_responses: List[Dict[str, Any]] = []
            listing_intercept_stats: Dict[str, int] = defaultdict(int)
            listing_intercept_examples: Dict[str, List[str]] = defaultdict(list)

            def _add_example(key: str, value: str) -> None:
                arr = listing_intercept_examples[key]
                if len(arr) < 4:
                    arr.append(value)

            def _on_listing_api(response: Any) -> None:
                url = response.url
                if "oddspedia" not in url.lower():
                    return

                listing_intercept_stats["oddspedia_seen"] += 1

                if not self._is_listing_api_endpoint(url):
                    listing_intercept_stats["filtered_non_listing"] += 1
                    _add_example("filtered_non_listing", url)
                    # DEBUG
                    if "getMatch" in url or "getAmerican" in url:
                        print(f"[scraper] DEBUG filtered match endpoint: {url[:150]}")
                    return
                # Skip per-match endpoints — those are handled later
                if "getMatchMaxOddsByGroup" in url:
                    listing_intercept_stats["filtered_per_match"] += 1
                    return

                if not response.ok:
                    listing_intercept_stats["non_ok_status"] += 1
                    _add_example("non_ok_status", f"{response.status} {url}")
                    return

                listing_intercept_stats["listing_candidate"] += 1

                body: Any = None
                try:
                    ct = response.headers.get("content-type", "")
                    if "json" in ct or "javascript" in ct:
                        body = response.json()
                    else:
                        # Some listing endpoints return JSON with generic content-types.
                        raw = response.text()
                        if raw and raw.strip().startswith(("{", "[")):
                            body = json.loads(raw)
                except Exception as exc:
                    listing_intercept_stats["json_parse_error"] += 1
                    _add_example("json_parse_error", f"{type(exc).__name__}: {url}")
                    body = None

                if isinstance(body, (dict, list)):
                    listing_intercept_stats["captured"] += 1
                    listing_api_responses.append({"url": url, "body": body})
                else:
                    listing_intercept_stats["non_json_body"] += 1
                    _add_example("non_json_body", url)

            page.on("response", _on_listing_api)

            # Prime session cookies via homepage before loading the target league page.
            # Cloudflare blocks direct cold loads on some league pages (e.g. EPL) —
            # hitting the homepage first establishes cf_clearance cookies.
            print(f"[scraper] Priming session via homepage...")
            page.goto("https://oddspedia.com", wait_until="domcontentloaded", timeout=self._page_timeout_ms)
            page.wait_for_timeout(3000)
            
            print(f"[scraper] Loading default listing page: {url}")
            page.goto(url, wait_until="domcontentloaded", timeout=self._page_timeout_ms)
            page.wait_for_timeout(5000)  # Extra wait for EPL page JS hydration
            try:
                page.wait_for_function(
                    "() => window.__NUXT__ && window.__NUXT__.data",
                    timeout=30000,
                )
            except Exception as wait_exc:
                print(f"[scraper] wait_for_function timed out ({wait_exc}); evaluating __NUXT__ as-is")


            # Simulate tab visibility change to trigger deferred API calls
            try:
                page.evaluate("""() => {
                    Object.defineProperty(document, 'visibilityState', {
                        value: 'hidden', writable: true
                    });
                    document.dispatchEvent(new Event('visibilitychange'));
                    setTimeout(() => {
                        Object.defineProperty(document, 'visibilityState', {
                            value: 'visible', writable: true
                        });
                        document.dispatchEvent(new Event('visibilitychange'));
                    }, 500);
                }""")
                print("[scraper] Dispatched visibility change events")
            except Exception as exc:
                print(f"[scraper] Visibility change failed: {exc}")
            
            # Allow time for deferred API calls to fire after visibility change
            page.wait_for_timeout(3000)

            # Explicitly fetch full match list since camoufox doesn't always capture
            # listing data via SSR/intercepts.
            #
            # For soccer league pages, category/league/season filters are usually valid.
            # For tennis tournament pages, Oddspedia can return HTTP 400 when those
            # filters are included (e.g. stale season ids). In that case we retry with
            # a sport-wide query and rely on downstream enrichment/filtering.
            try:
                import urllib.parse
                from datetime import datetime, timezone, timedelta
                now = datetime.now(timezone.utc)
                start = (now - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
                end = (now + timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ")

                def _fetch_listing(url: str) -> Dict[str, Any]:
                    return page.evaluate(
                        """async (url) => {
                            const res = await fetch(url, {
                                credentials: 'include',
                                redirect: 'follow',
                                headers: { 'accept': 'application/json, text/plain, */*' }
                            });
                            const text = await res.text();
                            let data = null;
                            if (res.ok) {
                                try { data = JSON.parse(text); } catch (e) { data = null; }
                            }
                            return { status: res.status, body: text.slice(0, 500), data };
                        }""",
                        url,
                    )

                def _build_match_list_url(*, filtered: bool) -> str:
                    base = (
                        f"https://oddspedia.com/api/v1/getMatchList"
                        f"?excludeSpecialStatus=0&sortBy=default&perPageDefault=50"
                        f"&startDate={urllib.parse.quote(start)}&endDate={urllib.parse.quote(end)}"
                        f"&geoCode=US&status=all&sport={sport}&popularLeaguesOnly=0"
                    )
                    if filtered:
                        base += (
                            f"&category={league_category}&league={league_slug}"
                            f"&seasonId={season_id}&round="
                        )
                    return f"{base}&r=wv&page=1&perPage=50&language=us"

                def _build_american_odds_url() -> str:
                    return (
                        f"https://oddspedia.com/api/v1/getAmericanMaxOddsWithPagination"
                        f"?geoCode=US&bookmakerGeoCode=US&bookmakerGeoState=NY&wettsteuer=0"
                        f"&startDate={urllib.parse.quote(start)}&endDate={urllib.parse.quote(end)}"
                        f"&sport={sport}&ot=201&excludeSpecialStatus=0&popularLeaguesOnly=0"
                        f"&sortBy=default&status=all&page=1&perPage=50&r=si&inplay=0&language=us"
                    )

                if sport == "tennis":
                    request_plan = [
                        ("sport_only", _build_match_list_url(filtered=False)),
                        ("american_odds", _build_american_odds_url()),
                    ]
                else:
                    request_plan = [
                        ("filtered", _build_match_list_url(filtered=True))
                    ]
                # ✅ ADD THESE 2 LINES RIGHT HERE
                print(f"[scraper] USING SPORT MODE: {sport}")
                print(f"[scraper] REQUEST PLAN: {[x[0] for x in request_plan]}")

                for label, url in request_plan:
                    ml_result = _fetch_listing(url)
                    print(f"[scraper] direct fetch ({label}) status: {ml_result.get('status')}")
                    print(f"[scraper] direct fetch ({label}) response body: {ml_result.get('body')}")

                    if ml_result.get("status") == 200 and ml_result.get("data"):
                        listing_api_responses.append({"url": url, "body": ml_result["data"]})
                        break
            except Exception as exc:
                print(f"[scraper] getMatchList direct fetch error: {exc}")

            if listing_intercept_stats:
                stats_line = ", ".join(f"{k}={v}" for k, v in sorted(listing_intercept_stats.items()))
                print(f"[scraper] Listing intercept stats: {stats_line}")
                for key in ("filtered_non_listing", "json_parse_error", "non_ok_status", "non_json_body"):
                    samples = listing_intercept_examples.get(key) or []
                    if samples:
                        print(f"[scraper]   {key} samples: {samples}")

            nuxt_data = page.evaluate("() => window.__NUXT__ || {}")
            # Only raise if __NUXT__ is entirely missing (Cloudflare challenge).
            # data: [] means client-side page — handled below via API interception.
            if "data" not in (nuxt_data or {}):
                print(
                    f"[scraper] window.__NUXT__ has no 'data' key — falling back to "
                    f"intercepted API responses. Top-level __NUXT__ keys: {list((nuxt_data or {}).keys())}"
                )

            records = self._build_records_from_nuxt(nuxt_data)
            print(f"[scraper] Default page: {len(records)} matches (SSR)")

            # If SSR gave no match list, fall back to intercepted API responses
            if not records:
                print(
                    f"[scraper] SSR matchList empty — trying "
                    f"{len(listing_api_responses)} intercepted listing-page API responses"
                )
                records = self._build_records_from_api_responses(listing_api_responses)
                print(f"[scraper] Built {len(records)} matches from intercepted API responses")

            # Final fallback: scrape rendered match links from the DOM
            if not records:
                print("[scraper] API fallback empty — trying DOM link extraction")
                records = self._build_records_from_dom(page)
                print(f"[scraper] Built {len(records)} matches from DOM links")

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

            # ── Per-match odds via match-page navigation + response interception ──
            # Navigate to each individual match URL.  The page's own JavaScript
            # fires getMatchMaxOddsByGroup XHR calls using Chrome's real network
            # stack (proper TLS fingerprint, Sec-Fetch-* headers, valid session
            # cookies).  We intercept those responses before they reach the JS.
            # This bypasses the Cloudflare WAF that blocks Python-level HTTP
            # requests (page.request.get / fetch from page context).
            #
            # Probe strategy: after the first match page, check if any API
            # responses were captured.  If not, stop early rather than loading
            # 50 pages that won't produce data.

            import re

            match_urls = self._build_all_match_urls(nuxt_data)
            print(
                f"[scraper] Built match URLs for {len(match_urls)}/{len(records)} matches"
            )

            captured: Dict[str, Dict] = defaultdict(dict)
            # Non-odds match-page responses (match info, stats, etc.) keyed by match_id
            match_info_captured: Dict[str, Dict] = {}

            def _on_api_response(response: Any) -> None:
                if "oddspedia.com" not in response.url:
                    return
                try:
                    mid_m = re.search(r"matchId=(\d+)", response.url)
                    if not mid_m:
                        return
                    mid_str = mid_m.group(1)

                    if "getMatchMaxOddsByGroup" in response.url:
                        mg_m = re.search(r"marketGroupId=(\d+)", response.url)
                        if not mg_m:
                            return
                        if response.ok:
                            captured[mid_str][mg_m.group(1)] = response.json()
                        else:
                            print(
                                f"[scraper] intercept HTTP {response.status} "
                                f"matchId={mid_str} mgId={mg_m.group(1)}"
                            )
                    elif response.ok and mid_str not in match_info_captured:
                        # Capture the first non-odds response per match for team enrichment
                        ct = response.headers.get("content-type", "")
                        if "json" in ct or "javascript" in ct:
                            try:
                                match_info_captured[mid_str] = response.json()
                            except Exception:
                                pass
                except Exception:
                    pass

            page.on("response", _on_api_response)

            total_market_rows = 0
            api_working: Optional[bool] = None  # None=untested, True=working, False=blocked

            # Enrich match records with team names/dates via getMatchInfo
            for record in records:
                mid = str(record.get("match_id") or "")
                if not mid:
                    continue
                try:
                    info_url = (
                        f"https://oddspedia.com/api/v1/getMatchInfo"
                        f"?matchId={mid}&language=us&geoCode=US"
                    )
                    info_result = page.evaluate(
                        """async (url) => {
                            const res = await fetch(url, {
                                credentials: 'include',
                                redirect: 'follow',
                                headers: { 'accept': 'application/json, text/plain, */*' }
                            });
                            if (!res.ok) return { status: res.status };
                            return { status: 200, data: await res.json() };
                        }""",
                        info_url
                    )
                    if info_result.get("status") == 200:
                        d = (info_result.get("data") or {}).get("data") or {}
                        record["home_team"] = d.get("ht")
                        record["away_team"] = d.get("at")
                        record["home_team_id"] = d.get("ht_id")
                        record["away_team_id"] = d.get("at_id")
                        record["league_id"] = d.get("league_id")
                        record["date_utc"] = _normalise_ts(d.get("starttime") or d.get("md"))
                        record["match_info"] = d  # store full payload for downstream ingest
                        print(f"[scraper] match={mid} enriched: {d.get('ht')} vs {d.get('at')} @ {record['date_utc']}")

                        # Capture stats via direct API calls
                        try:
                            mk = d.get("match_key")
                            stats_result = page.evaluate(
                                """async (matchKey) => {
                                    try {
                                        const base = 'https://oddspedia.com/api/v1';
                                        const lang = 'language=us';
                                        const [bs, pms, h2h, lmh, lma, st, lu] = await Promise.all([
                                            fetch(`${base}/getMatchBettingStats?matchKey=${matchKey}&${lang}`).then(r=>r.json()),
                                            fetch(`${base}/getPerMatchStats?matchKey=${matchKey}&${lang}`).then(r=>r.json()),
                                            fetch(`${base}/getHeadToHead?matchKey=${matchKey}&all=1&${lang}`).then(r=>r.json()),
                                            fetch(`${base}/getTeamLastMatches?matchKey=${matchKey}&type=home&teamId=0&upcomingMatchesLimit=2&finishedMatchesLimit=5&geoCode=US&${lang}`).then(r=>r.json()),
                                            fetch(`${base}/getTeamLastMatches?matchKey=${matchKey}&type=away&teamId=0&upcomingMatchesLimit=2&finishedMatchesLimit=5&geoCode=US&${lang}`).then(r=>r.json()),
                                            fetch(`${base}/getLeagueStandings?matchKey=${matchKey}&${lang}`).then(r=>r.json()),
                                            fetch(`${base}/getMatchLineUps?matchKey=${matchKey}&${lang}`).then(r=>r.json()),
                                        ]);
                                        return { bs, pms, h2h, lmh, lma, st, lu };
                                    } catch(e) {
                                        return { error: e.toString() };
                                    }
                                }""",
                                mk
                            )
                            if stats_result and not stats_result.get("error"):
                                record["betting_stats"]     = stats_result.get("bs", {}).get("data")
                                record["per_match_stats"]   = stats_result.get("pms", {}).get("data")
                                record["head_to_head"]      = stats_result.get("h2h", {}).get("data")
                                record["last_matches_home"] = stats_result.get("lmh", {}).get("data")
                                record["last_matches_away"] = stats_result.get("lma", {}).get("data")
                                record["standings_data"]    = stats_result.get("st", {}).get("data")
                                record["lineups"]           = stats_result.get("lu", {}).get("data")
                                print(f"[scraper] match={mid} stats captured via API")
                            else:
                                print(f"[scraper] match={mid} stats API error: {stats_result}")
                        except Exception as exc_bs:
                            print(f"[scraper] match={mid} stats capture error: {exc_bs}")


                    else:
                        print(f"[scraper] getMatchInfo match={mid} status={info_result.get('status')}")
                except Exception as exc:
                    print(f"[scraper] getMatchInfo match={mid} error: {exc}")

            print(f"[scraper] Firing per-match API calls from listing page context ({len(records)} matches)")

            for record in records:
                mid = str(record.get("match_id") or "")
                if not mid:
                    continue

                all_rows: List[Dict[str, Any]] = []

                for mg in _PER_MATCH_MARKET_GROUPS:
                    api_url = (
                        f"{_PER_MATCH_API}"
                        f"?matchId={mid}"
                        f"&marketGroupId={mg}"
                        f"&{_PER_MATCH_PARAMS}"
                    )
                    try:
                        result = page.evaluate(
                            """async (url) => {
                                try {
                                    const res = await fetch(url, {
                                        credentials: 'include',
                                        redirect: 'follow',
                                        headers: { 'accept': 'application/json, text/plain, */*' }
                                    });
                                    if (!res.ok) return { status: res.status };
                                    return { status: 200, data: await res.json() };
                                } catch (e) {
                                    return { status: 0, error: e.toString() };
                                }
                            }""",
                            api_url
                        )

                        status = result.get("status") if isinstance(result, dict) else None
                        error = result.get("error") if isinstance(result, dict) else None
                        print(f"[scraper] match={mid} mg={mg} status={status}" + (f" error={error}" if error else ""))

                        if status == 200 and result.get("data"):
                            rows = self._parse_per_match_to_market_rows(result["data"], mid)
                            all_rows.extend(rows)
                            print(f"[scraper]   → {len(rows)} rows")

                    except Exception as exc:
                        print(f"[scraper] match={mid} mg={mg} error: {exc}")

                if all_rows:
                    record["market_rows"] = all_rows
                    total_market_rows += len(all_rows)


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

    def fetch_h2h(
        self,
        match_ids: List[int],
    ) -> Dict[int, Any]:
        """
        Fetch head-to-head data for a list of match IDs.
        
        For each match ID:
          1. Calls getMatchInfo to get ht_slug, at_slug, match_key
          2. Loads the match insights page via camoufox
          3. Extracts headToHead from the __NUXT__ SSR blob
        
        Returns {match_id: headToHead_dict}
        """
        from camoufox.sync_api import Camoufox
    
        results: Dict[int, Any] = {}
    
        with Camoufox(headless=True, geoip=True) as browser:
            context = browser.new_context(
                locale="en-US",
                viewport={"width": 1920, "height": 1080},
                extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
            )
            page = context.new_page()
    
            # Use a dummy page load to establish session cookies
            page.goto("https://oddspedia.com", wait_until="domcontentloaded", timeout=self._page_timeout_ms)
            page.wait_for_timeout(2000)
    
            for match_id in match_ids:
                try:
                    # Step 1: get slugs from getMatchInfo
                    info_url = (
                        f"https://oddspedia.com/api/v1/getMatchInfo"
                        f"?matchId={match_id}&language=us&geoCode=US"
                    )
                    info_result = page.evaluate(
                        """async (url) => {
                            const res = await fetch(url, {
                                credentials: 'include',
                                headers: { 'accept': 'application/json, text/plain, */*' }
                            });
                            if (!res.ok) return { status: res.status };
                            return { status: 200, data: await res.json() };
                        }""",
                        info_url
                    )
    
                    if info_result.get("status") != 200:
                        print(f"[h2h] match={match_id} getMatchInfo status={info_result.get('status')} — skipping")
                        continue
    
                    d = (info_result.get("data") or {}).get("data") or {}
                    ht_slug = d.get("ht_slug")
                    at_slug = d.get("at_slug")
                    match_key = d.get("match_key")
                    ht = d.get("ht")
                    at = d.get("at")
    
                    if not ht_slug or not at_slug or not match_key:
                        print(f"[h2h] match={match_id} missing slugs/match_key — skipping")
                        continue
    
                    print(f"[h2h] match={match_id} fetching H2H: {ht} vs {at}")
    
                    # Step 2: load insights page
                    insights_url = (
                        f"https://oddspedia.com/football/"
                        f"{ht_slug}-{at_slug}-{match_key}?tab=insights"
                    )
                    page.goto(insights_url, wait_until="domcontentloaded", timeout=self._page_timeout_ms)
                    page.wait_for_timeout(3000)
    
                    # Step 3: extract headToHead from __NUXT__
                    h2h = page.evaluate("""() => {
                        for (const s of document.scripts) {
                            if (s.text && s.text.includes('window.__NUXT__')) {
                                try { eval(s.text); } catch(e) {}
                                break;
                            }
                        }
                        if (!window.__NUXT__) return null;
                        
                        function findKey(obj, key, depth) {
                            if (depth > 5 || !obj || typeof obj !== 'object') return null;
                            if (obj[key] !== undefined) return obj[key];
                            for (const k of Object.keys(obj)) {
                                const found = findKey(obj[k], key, depth + 1);
                                if (found) return found;
                            }
                            return null;
                        }
                        return findKey(window.__NUXT__, 'headToHead', 0);
                    }""")
    
                    if h2h:
                        results[match_id] = h2h
                        print(f"[h2h] match={match_id} got H2H: {h2h.get('played_matches')} matches played")
                    else:
                        print(f"[h2h] match={match_id} no headToHead found in __NUXT__")
    
                except Exception as exc:
                    print(f"[h2h] match={match_id} error: {exc}")
    
            browser.close()
    
        return results



    # ============================================================
    # PER-MATCH HELPERS
    # ============================================================

    def _build_all_match_urls(self, nuxt_data: Dict[str, Any]) -> Dict[str, str]:
        """
        Build {str(match_id): url} for every match in the listing-page NUXT.
        Falls back to constructing the slug URL when the raw url field is absent.
        """
        urls: Dict[str, str] = {}
        try:
            data0 = (nuxt_data.get("data") or [{}])[0]
            for m in (data0.get("matchList") or []):
                mid = str(
                    m.get("id") or m.get("match_id") or m.get("matchId") or ""
                )
                if not mid:
                    continue
                raw_url = m.get("url")
                if raw_url and isinstance(raw_url, str):
                    base = "https://www.oddspedia.com"
                    urls[mid] = (
                        raw_url if raw_url.startswith("http") else base + raw_url
                    )
                    continue
                ht_slug = m.get("ht_slug") or m.get("htSlug")
                at_slug = m.get("at_slug") or m.get("atSlug")
                if ht_slug and at_slug:
                    if "tennis" in (nuxt_data.get("data", [{}])[0].get("currentSport", {}) or {}).get("slug", ""):
                        urls[mid] = f"https://www.oddspedia.com/us/a/tennis/{ht_slug}-{at_slug}-{mid}"
                    else:
                        urls[mid] = f"https://www.oddspedia.com/us/soccer/mls/{ht_slug}-{at_slug}-{mid}"
        except Exception as exc:
            print(f"[scraper] _build_all_match_urls error: {exc}")
        return urls

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
    # LISTING-PAGE API FALLBACK
    # ============================================================

    def _build_records_from_api_responses(
        self,
        api_responses: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Build match records from intercepted listing-page API responses.
        Used when the page does not SSR the match list into __NUXT__.data
        (e.g. Oddspedia MLS soccer page loads matches client-side).

        Tries several common response shapes to find a match list.
        """
        # DEBUG — remove after diagnosis
        for resp in api_responses:
            print(f"[scraper] captured endpoint: {resp.get('url', '')[:120]}")
            # DEBUG body shape
            body = resp.get("body", {})
            print(f"[scraper]   raw body type: {type(body)}")
            if isinstance(body, dict):
                print(f"[scraper]   body top-level keys: {list(body.keys())[:15]}")
                data = body.get("data") or {}
                if isinstance(data, dict):
                    print(f"[scraper]   body.data keys: {list(data.keys())[:15]}")
                elif isinstance(data, list):
                    print(f"[scraper]   body.data is a list, len={len(data)}, first item keys: {list(data[0].keys())[:10] if data else 'empty'}")
            elif isinstance(body, list):
                print(f"[scraper]   body is a list, len={len(body)}, first item keys: {list(body[0].keys())[:10] if body else 'empty'}")
            # DEBUG match value shape
            if isinstance(data, dict) and data and all(str(k).isdigit() for k in list(data.keys())[:3]):
                first_val = list(data.values())[0]
                print(f"[scraper]   first match value keys: {list(first_val.keys())[:15] if isinstance(first_val, dict) else first_val}")

        for resp in api_responses:
            endpoint = resp.get("url", "")
            if not self._is_listing_api_endpoint(endpoint):
                continue

            body = resp.get("body", {})
            if not isinstance(body, (dict, list)):
                continue

            # Unwrap common envelope shapes
            data: Any = body
            if isinstance(body, dict):
                data = body.get("data") or body

            # Some endpoints nest under a second "data" key
            if isinstance(data, dict) and "data" in data and isinstance(data["data"], dict):
                data = data["data"]

            # Look for a list of match-like objects
            candidates = []
            if isinstance(data, dict):
                candidates.extend([
                    data.get("matchList"),
                    data.get("matches"),
                    data.get("items"),
                    data.get("results"),
                    data.get("smartBets"),
                    data.get("list"),
                ])
            if isinstance(body, dict):
                candidates.extend([
                    body.get("matchList"),
                    body.get("matches"),
                ])
            if isinstance(data, list):
                candidates.append(data)
            if isinstance(body, list):
                candidates.append(body)

            # Handle getMatchListSmartBets shape: body.data = {'9780963': {...}, ...}
            if isinstance(data, dict) and data and all(str(k).isdigit() for k in list(data.keys())[:3]):
                print(f"[scraper] Detected match-keyed dict shape, {len(data)} entries")
                candidates.append([
                    {**v, "id": k} if isinstance(v, dict) else {"id": k}
                    for k, v in data.items()
                ])


            for candidate in candidates:
                if not (isinstance(candidate, list) and candidate):
                    continue
                # Confirm it looks like a match list (has id + team slugs)
                normalized = [
                    self._normalise_listing_match(item)
                    for item in candidate
                    if isinstance(item, dict)
                ]
                normalized = [item for item in normalized if item and item.get("id")]
                if not normalized:
                    continue

                print(
                    f"[scraper] Listing API match list found — "
                    f"{len(normalized)} items from {endpoint[:80]}"
                )
                odds_data = {}
                if isinstance(data, dict):
                    odds_data = data.get("odds") or {}
                if not odds_data and isinstance(body, dict):
                    odds_data = body.get("odds") or {}
                raw = {"matchList": normalized, "odds": odds_data, "sport": "soccer"}
                built = self._build_records(raw)
                if built:
                    return built

            # Broader fallback: recursively scan payload for match-like objects
            # in case Oddspedia changes envelope keys for listing endpoints.
            scanned: List[Dict[str, Any]] = []
            for item in self._extract_match_candidates(data):
                normalized_item = self._normalise_listing_match(item)
                if normalized_item and normalized_item.get("id"):
                    scanned.append(normalized_item)
            if scanned:
                deduped: Dict[str, Dict[str, Any]] = {}
                for item in scanned:
                    deduped[str(item["id"])] = item
                normalized = list(deduped.values())
                print(
                    f"[scraper] Listing API recursive scan found — "
                    f"{len(normalized)} items from {endpoint[:80]}"
                )
                raw_odds = data.get("odds") if isinstance(data, dict) else {}
                raw = {"matchList": normalized, "odds": raw_odds or {}, "sport": "soccer"}
                built = self._build_records(raw)
                if built:
                    return built

        print("[scraper] No match list found in intercepted listing-page API responses")
        return []


    def _is_listing_api_endpoint(self, endpoint: str) -> bool:
        """Return True for likely listing endpoints and reject telemetry/ads hosts."""
        try:
            parsed = urlparse(endpoint)
            host = (parsed.hostname or "").lower()
            path = parsed.path or ""
            query = parsed.query or ""
        except Exception:
            return False

        if not host:
            return False

        # Keep this permissive: Oddspedia sometimes serves APIs from different
        # oddspedia.com subdomains in CI/runners.
        if not host.endswith("oddspedia.com"):
            return False

        # Explicitly reject known telemetry/identity hosts.
        blocked_prefixes = (
            "smetrics.",
            "metrics.",
            "analytics.",
            "pixel.",
            "tags.",
        )
        if host.startswith(blocked_prefixes):
            return False

        if "getMatchMaxOddsByGroup" in endpoint:
            return False

        # Reject non-match-list endpoints that return league/bookie metadata
        endpoint_l = endpoint.lower()
        if "getleagues" in endpoint_l:
            return False
        if "getoutrights" in endpoint_l:
            return False
        if "getbookmakers" in endpoint_l:
            return False
        if "getleaguelivestream" in endpoint_l:
            return False
        if "getcategories" in endpoint_l:
            return False
        

        # Accept common listing routes, but don't hard-require /api/.
        if "/api/" in path:
            # Ignore per-match API calls in listing capture stage.
            if "matchid=" in query.lower() and "getmatchodds" in endpoint_l:
                return False
            return True
        if "getamericanmaxoddswithpagination" in endpoint_l:
            return True

        # MLS / soccer listing endpoint
        if "getmatchlist" in endpoint_l:
            return True

        # generic odds listing endpoints
        if "getmatchodds" in endpoint_l and "matchid=" not in query.lower():
            return True

        return False

    def _extract_match_candidates(self, node: Any) -> List[Dict[str, Any]]:
        """Recursively collect dicts that look like match rows from API JSON."""
        out: List[Dict[str, Any]] = []
        stack: List[Any] = [node]
        while stack:
            cur = stack.pop()
            if isinstance(cur, dict):
                if self._looks_like_match_row(cur):
                    out.append(cur)
                for value in cur.values():
                    if isinstance(value, (dict, list)):
                        stack.append(value)
            elif isinstance(cur, list):
                for value in cur:
                    if isinstance(value, (dict, list)):
                        stack.append(value)
        return out

    def _looks_like_match_row(self, row: Dict[str, Any]) -> bool:
        """Guard against false positives from analytics/identity payloads."""
        match_id = (
            row.get("id")
            or row.get("match_id")
            or row.get("matchId")
            or row.get("event_id")
            or row.get("game_id")
        )
        if not self._as_int(match_id):
            return False

        has_teams = any(
            row.get(k)
            for k in (
                "ht",
                "at",
                "home_team",
                "away_team",
                "homeTeam",
                "awayTeam",
                "home_name",
                "away_name",
                "home",
                "away",
            )
        )
        if has_teams:
            return True

        has_match_url = any(
            isinstance(row.get(k), str) and "/soccer/" in row.get(k)
            for k in ("url", "match_url", "path")
        )
        return has_match_url

    def _as_int(self, value: Any) -> Optional[int]:
        try:
            return int(str(value))
        except (TypeError, ValueError):
            return None

    def _normalise_listing_match(self, match: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Normalize variable listing-API match keys to Oddspedia `matchList` shape."""
        match_id = (
            match.get("id")
            or match.get("match_id")
            or match.get("matchId")
            or match.get("event_id")
            or match.get("game_id")
        )
        match_id_int = self._as_int(match_id)
        if match_id_int is None:
            return None

        home_team = (
            match.get("ht")
            or match.get("home_team")
            or match.get("homeTeam")
            or match.get("home")
            or match.get("home_name")
        )
        away_team = (
            match.get("at")
            or match.get("away_team")
            or match.get("awayTeam")
            or match.get("away")
            or match.get("away_name")
        )

        return {
            "id": match_id_int,
            "md": match.get("md") or match.get("starttime") or match.get("start_time") or match.get("date"),
            "ht": home_team,
            "at": away_team,
            "ht_id": match.get("ht_id") or match.get("home_team_id") or match.get("homeTeamId"),
            "at_id": match.get("at_id") or match.get("away_team_id") or match.get("awayTeamId"),
            "inplay": match.get("inplay") or match.get("is_live") or False,
            "league_id": match.get("league_id") or match.get("leagueId"),
            "url": match.get("url") or match.get("match_url") or match.get("path"),
            "ht_slug": match.get("ht_slug") or match.get("htSlug") or match.get("home_slug"),
            "at_slug": match.get("at_slug") or match.get("atSlug") or match.get("away_slug"),
        }

    def _build_records_from_dom(self, page: Any) -> List[Dict[str, Any]]:
        """
        Last-resort match discovery: find rendered match links in the DOM.
        Works for client-side pages (like MLS) where neither SSR nor API
        interception yields a match list.
        """
        import re as _re
        try:
            # Give Vue/React a moment to finish rendering cards
            try:
                page.wait_for_timeout(3000)
            except Exception:
                pass

            # Evaluate in browser context: collect all hrefs that look like
            # match pages (contain /soccer/ and end with a long numeric ID)
            links: List[str] = page.evaluate(
                """() => Array.from(document.querySelectorAll('a[href]'))
                        .map(a => a.href)
                        .filter(h => /\\/soccer\\//.test(h) && /\\/\\d{6,}(\\?.*)?$/.test(h))"""
            ) or []

            seen: set = set()
            records: List[Dict[str, Any]] = []
            for href in links:
                m = _re.search(r"/(\d{6,})(?:[/?]|$)", href)
                if not m:
                    continue
                match_id = int(m.group(1))
                if match_id in seen:
                    continue
                seen.add(match_id)
                full_url = href.split("?")[0].rstrip("/")
                records.append(
                    {
                        "match_id": match_id,
                        "sport": "soccer",
                        "date_utc": None,
                        "home_team": None,
                        "away_team": None,
                        "home_team_id": None,
                        "away_team_id": None,
                        "inplay": False,
                        "league_id": None,
                        "url": full_url,
                        "markets": {},
                    }
                )

            print(
                f"[scraper] DOM link scan: {len(links)} links → "
                f"{len(records)} unique match IDs"
            )
            return records
        except Exception as exc:
            print(f"[scraper] DOM link extraction failed: {exc}")
            return []

    def _enrich_record_from_match_page(
        self,
        record: Dict[str, Any],
        mid: str,
        page: Any,
        match_info_captured: Dict[str, Any],
    ) -> None:
        """
        Fill in home_team / away_team / date_utc on a record that was built
        from a DOM link (and therefore has no team info yet).

        Tries two sources in order:
          1. Intercepted non-odds API response for this match
          2. window.__NUXT__ on the current (match) page
        """
        import re as _re

        def _apply(data: Dict[str, Any]) -> bool:
            ht = data.get("ht") or data.get("home_team") or data.get("homeTeam")
            at = data.get("at") or data.get("away_team") or data.get("awayTeam")
            md = data.get("md") or data.get("starttime") or data.get("start_time")
            lid = data.get("league_id") or data.get("leagueId")
            if ht or at:
                record["home_team"] = record.get("home_team") or ht
                record["away_team"] = record.get("away_team") or at
                if md:
                    record["date_utc"] = record.get("date_utc") or _normalise_ts(str(md))
                if lid:
                    record["league_id"] = record.get("league_id") or lid
                return True
            return False

        # Source 1: intercepted API response
        info = match_info_captured.get(mid, {})
        if isinstance(info, dict):
            inner = info.get("data") or info
            if isinstance(inner, dict) and _apply(inner):
                return

        # Source 2: __NUXT__ on the match page
        try:
            nuxt = page.evaluate("() => window.__NUXT__ || {}") or {}
            data0 = (nuxt.get("data") or [{}])[0]
            for key in ("matchData", "match", "matchInfo", "currentMatch"):
                candidate = data0.get(key)
                if isinstance(candidate, dict) and _apply(candidate):
                    return
            # Some pages put match info directly in data[0]
            if isinstance(data0, dict) and _apply(data0):
                return
        except Exception:
            pass

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
                if raw_url and isinstance(raw_url, str):
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

            raw_url = match.get("url")
            base = "https://www.oddspedia.com"
            record_url: Optional[str] = None
            if raw_url and isinstance(raw_url, str):
                record_url = raw_url if raw_url.startswith("http") else base + raw_url

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
                "url": record_url,
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

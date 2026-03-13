"""
Discover all Oddspedia tennis odds endpoints and market IDs, including set betting.

Three-phase approach:
  Phase 1 – Listing page:  load https://www.oddspedia.com/us/tennis/odds,
                            extract match list + IDs from __NUXT__.
  Phase 2 – Match page:    navigate to a real match page, capture all XHR
                            via on_response, extract SSR payload from
                            <script type="application/json"> tags, collect
                            cookies for Phase 3.
  Phase 3 – Python requests: use the browser's cookies (harvested in Phase 2)
                            in a Python requests.Session so we have the full
                            auth context without CORS/browser-context issues.
                            a) Diagnostic: print what ot=None returns.
                            b) Sweep ot=100..2000 on getAmericanMaxOddsWithPagination.
                            c) Probe getAmericanMaxOddsWithPagination WITH matchId.
                            d) Probe match-specific endpoints.

Run from a Codespace/machine that can reach oddspedia.com.
"""
import json
import re
import time
from urllib.parse import urlparse

import requests as req_lib
from playwright.sync_api import sync_playwright

try:
    from playwright_stealth import stealth_sync as stealth
except ImportError:
    stealth = None

LISTING_URL = "https://www.oddspedia.com/us/tennis/odds"
UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
_HEADLESS_SHELL = (
    "/root/.cache/ms-playwright/chromium_headless_shell-1194"
    "/chrome-linux/headless_shell"
)
EXCLUDE_PATH_TOKENS = {"picks", "odds-explained", "predictions", "news", "highlights"}

KNOWN_MARKETS = {
    "201": "Moneyline",
    "301": "Spread",
    "401": "Total (sets O/U)",
}


# ── helpers ───────────────────────────────────────────────────────────────────

def _tennis_segments(path):
    parts = [p for p in path.strip("/").split("/") if p]
    if "tennis" in parts:
        return parts[parts.index("tennis") + 1:]
    return parts


def looks_like_match_path(url):
    parsed = urlparse(url)
    if "/us/tennis/" not in parsed.path or "#" in url:
        return False
    segs = _tennis_segments(parsed.path)
    if not segs:
        return False
    slug = segs[-1]
    if slug in EXCLUDE_PATH_TOKENS | {"odds", "live", "results", "fixtures"}:
        return False
    return bool(re.search(r"[a-z0-9]+-[a-z0-9]+", slug)) and slug.count("-") >= 1


def build_match_candidates_from_data(match_list):
    candidates = []
    for item in match_list:
        raw_url = item.get("url")
        if isinstance(raw_url, str) and raw_url:
            candidates.append(
                raw_url if raw_url.startswith("http")
                else f"https://www.oddspedia.com{raw_url if raw_url.startswith('/') else '/' + raw_url}"
            )
        ht, at = item.get("ht_slug"), item.get("at_slug")
        eid = item.get("id") or item.get("match_id")
        if ht and at:
            slug = f"{ht}-{at}" + (f"-{eid}" if eid else "")
            candidates.append(f"https://www.oddspedia.com/us/tennis/{slug}")
    return list(dict.fromkeys(candidates))


def _walk_for_keys(obj, targets, depth=0, max_depth=8):
    if depth > max_depth or not isinstance(obj, (dict, list)):
        return {}
    found = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in targets:
                found.setdefault(k, v)
            found.update(_walk_for_keys(v, targets, depth + 1, max_depth))
    else:
        for item in obj[:20]:
            found.update(_walk_for_keys(item, targets, depth + 1, max_depth))
    return found


def _extract_market_ids(body):
    mids = set()
    if not isinstance(body, dict):
        return mids
    d = body.get("data", body)
    markets = d.get("markets", {}) if isinstance(d, dict) else {}
    if isinstance(markets, dict):
        mids.update(str(k) for k in markets.keys())
    elif isinstance(markets, list):
        for m in markets:
            if isinstance(m, dict):
                for key in ("id", "marketTypeId", "market_id", "ot"):
                    if key in m:
                        mids.add(str(m[key]))
    matches = d.get("matches", {}) if isinstance(d, dict) else {}
    if isinstance(matches, dict):
        for mdata in matches.values():
            if isinstance(mdata, dict):
                mids.update(str(k) for k in mdata.keys())
    return mids


def _req_json(session, url, retries=2):
    """GET url with the requests session, return (body_dict_or_None, error_str)."""
    for attempt in range(retries + 1):
        try:
            r = session.get(url, timeout=15)
            if r.status_code == 429:
                time.sleep(5 * (attempt + 1))
                continue
            if r.status_code != 200:
                return None, str(r.status_code)
            return r.json(), None
        except Exception as e:
            if attempt < retries:
                time.sleep(2)
            else:
                return None, str(e)
    return None, "max_retries"


# ── main ──────────────────────────────────────────────────────────────────────

import os
_shell = _HEADLESS_SHELL if os.path.exists(_HEADLESS_SHELL) else None

with sync_playwright() as pw:
    browser = pw.chromium.launch(headless=True, executable_path=_shell)
    ctx = browser.new_context(user_agent=UA, locale="en-US")
    page = ctx.new_page()
    if stealth:
        stealth(page)

    # ── Phase 1: listing page ─────────────────────────────────────────────────
    print(f"\n{'='*70}")
    print(f"Phase 1 — listing page: {LISTING_URL}")
    page.goto(LISTING_URL, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_function("() => !!window.__NUXT__", timeout=20000)

    nuxt = page.evaluate("() => window.__NUXT__")
    data0 = (nuxt.get("data") or [{}])[0] or {}
    match_list = data0.get("matchList", [])
    print(f"  matchList entries: {len(match_list)}")

    match_ids = []
    for m in match_list:
        mid = m.get("id") or m.get("match_id") or m.get("matchId")
        if mid:
            match_ids.append(str(mid))
    print(f"  match IDs found: {len(match_ids)}  (first few: {match_ids[:5]})")

    upcoming_slugs = {
        v for m in match_list
        if m.get("winner") is None and m.get("matchstatus") == 1
        for k in ("ht_slug", "at_slug", "slug", "url")
        for v in [m.get(k)] if isinstance(v, str) and v
    }

    all_links = page.evaluate(
        "() => Array.from(document.querySelectorAll('a[href]'))"
        ".map(a => a.href).filter(h => h.includes('/us/tennis/'))"
    )
    data_cands = [u for u in build_match_candidates_from_data(match_list) if looks_like_match_path(u)]
    dom_cands  = [u for u in list(dict.fromkeys(all_links)) if looks_like_match_path(u)]
    match_candidates = list(dict.fromkeys(data_cands + dom_cands))
    print(f"  match candidates: {len(match_candidates)}")

    match_url = next(
        (lnk for lnk in match_candidates
         if any(slug and any(slug in p for p in _tennis_segments(urlparse(lnk).path))
                for slug in upcoming_slugs)),
        match_candidates[0] if match_candidates else None
    )
    if not match_url:
        raise RuntimeError("Could not find a match URL.")
    print(f"  selected match: {match_url}")

    url_match_id = None
    id_match = re.search(r"-(\d{5,})$", urlparse(match_url).path)
    if id_match:
        url_match_id = id_match.group(1)
        if url_match_id not in match_ids:
            match_ids.insert(0, url_match_id)
    print(f"  match ID from URL: {url_match_id}")

    # ── Phase 2: match page ───────────────────────────────────────────────────
    print(f"\n{'='*70}")
    print("Phase 2 — match page (XHR capture + SSR payload extraction)")

    all_responses: dict[str, dict] = {}

    def on_response(response):
        if response.request.resource_type in ("image", "font", "stylesheet", "media"):
            return
        ctype = response.headers.get("content-type", "")
        body = None
        if "json" in ctype and response.status == 200:
            try:
                body = response.json()
            except Exception:
                pass
        if body is not None or "/api/" in response.url:
            all_responses[response.url] = {
                "status": response.status,
                "resource_type": response.request.resource_type,
                "json_body": body,
            }

    page.on("response", on_response)
    page.goto(match_url, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(8000)

    # Click market tabs to trigger lazy loads
    for click_text in ("More markets", "All markets", "Set betting", "Sets",
                       "1st Set", "2nd Set", "Correct Score", "Games"):
        try:
            page.click(f"text={click_text}", timeout=2000)
            page.wait_for_timeout(1500)
        except Exception:
            pass

    # Extract SSR payload (Nuxt 3 stores data in <script type="application/json">)
    nuxt_payloads = page.evaluate("""() => {
        const results = {};
        for (const key of ['__NUXT__', '__NUXT_DATA__', '__NUXT_PAYLOAD__',
                           '__nuxt_payload', '__initial_state__']) {
            if (window[key] && typeof window[key] === 'object') {
                results[key] = window[key];
            }
        }
        document.querySelectorAll('script[type="application/json"]').forEach((s, i) => {
            try { results['script_json_' + (s.id || i)] = JSON.parse(s.textContent); }
            catch(e) {}
        });
        const nd = document.getElementById('__NUXT_DATA__') ||
                   document.getElementById('__nuxt_data__');
        if (nd) {
            try { results['NUXT_DATA_tag'] = JSON.parse(nd.textContent); }
            catch(e) { results['NUXT_DATA_tag_raw'] = nd.textContent.slice(0, 500); }
        }
        return results;
    }""")

    # Harvest cookies AFTER match page is loaded (has full auth state)
    raw_cookies = ctx.cookies(urls=["https://www.oddspedia.com"])
    print(f"  cookies harvested: {len(raw_cookies)}")

    json_hits = {u: r for u, r in all_responses.items() if r["json_body"] is not None}
    api_hits  = {u: r for u, r in json_hits.items() if "/api/" in u}
    print(f"  JSON responses captured: {len(json_hits)}  API: {len(api_hits)}")

    all_match_market_ids: set[str] = set()
    for url, r in api_hits.items():
        parsed = urlparse(url)
        mids = _extract_market_ids(r["json_body"])
        all_match_market_ids.update(mids)
        d = r["json_body"].get("data", r["json_body"]) if isinstance(r["json_body"], dict) else {}
        print(f"\n  {parsed.path}")
        print(f"    query : {parsed.query[:120]}")
        print(f"    status: {r['status']}  markets: {sorted(mids)}")

    NUXT_TARGETS = {
        "markets", "odds", "matchOdds", "oddsTypes", "marketTypes", "oddsData",
        "oddsMarkets", "betTypes", "betOffers", "marketGroups",
    }
    print(f"\n  SSR sources: {list(nuxt_payloads.keys())}")
    for src_key, src_val in nuxt_payloads.items():
        nuxt_found = _walk_for_keys(src_val, NUXT_TARGETS)
        if nuxt_found:
            print(f"  [{src_key}] market keys: {list(nuxt_found.keys())}")
            for k, v in nuxt_found.items():
                if isinstance(v, dict):
                    print(f"    [{k}]: dict keys={list(v.keys())[:20]}")
                    all_match_market_ids.update(str(x) for x in v.keys() if str(x).isdigit())
        else:
            if isinstance(src_val, dict):
                print(f"  [{src_key}]: top keys={list(src_val.keys())[:10]}")

    browser.close()

# ── Phase 3: Python requests with harvested cookies ───────────────────────────
print(f"\n{'='*70}")
print("Phase 3 — Python requests session with match-page cookies")

session = req_lib.Session()
session.headers.update({
    "User-Agent": UA,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": match_url,
    "X-Requested-With": "XMLHttpRequest",
})
for c in raw_cookies:
    session.cookies.set(c["name"], c["value"])

from datetime import datetime, timezone, timedelta
today = datetime.now(timezone.utc).replace(hour=4, minute=0, second=0, microsecond=0)
tomorrow = today + timedelta(days=1) - timedelta(seconds=1)
date_start = today.strftime("%Y-%m-%dT%H:%M:%SZ")
date_end   = tomorrow.strftime("%Y-%m-%dT%H:%M:%SZ")

BASE_PARAMS = (
    f"geoCode=US&bookmakerGeoCode=US&bookmakerGeoState=VA&wettsteuer=0"
    f"&startDate={date_start}&endDate={date_end}&sport=tennis"
    f"&excludeSpecialStatus=0&popularLeaguesOnly=0&sortBy=default"
    f"&status=all&page=1&perPage=10&language=us"
)
BASE_URL = "https://www.oddspedia.com/api/v1/getAmericanMaxOddsWithPagination"

discovered_markets: dict[str, str] = dict(KNOWN_MARKETS)

# ── 3a: diagnostic no-filter call ─────────────────────────────────────────────
diag_body, diag_err = _req_json(session, f"{BASE_URL}?{BASE_PARAMS}")
if diag_body is None:
    print(f"  DIAGNOSTIC failed: {diag_err}")
else:
    d = diag_body.get("data", diag_body) if isinstance(diag_body, dict) else {}
    print(f"  DIAGNOSTIC response keys: {list(diag_body.keys())[:10]}")
    if isinstance(d, dict):
        print(f"  DIAGNOSTIC data keys: {list(d.keys())[:15]}")
        mkts = d.get("markets", {})
        print(f"  DIAGNOSTIC markets: type={type(mkts).__name__}  entries={list(mkts.keys())[:10] if isinstance(mkts, dict) else len(mkts) if isinstance(mkts, list) else '?'}")
        matches_ = d.get("matches", {})
        if isinstance(matches_, dict):
            first_m = next(iter(matches_), None)
            if first_m:
                print(f"  DIAGNOSTIC first match sub-keys: {list(matches_[first_m].keys())[:15]}")

# ── 3b: probe getAmericanMaxOddsWithPagination with matchId ───────────────────
print(f"\n  Probing with matchId...")
for probe_mid in (match_ids[:3] if match_ids else []):
    url = f"{BASE_URL}?matchId={probe_mid}&geoCode=US&bookmakerGeoCode=US&language=us"
    body, err = _req_json(session, url)
    if body is None:
        print(f"    matchId={probe_mid}  error: {err}")
        continue
    d = body.get("data", body) if isinstance(body, dict) else {}
    mids = _extract_market_ids(body)
    print(f"    matchId={probe_mid}  markets: {sorted(mids)}  data keys: {list(d.keys())[:10] if isinstance(d, dict) else type(d).__name__}")
    new_mids = mids - set(discovered_markets)
    if new_mids:
        mkts = d.get("markets", {}) if isinstance(d, dict) else {}
        for nmid in sorted(new_mids):
            mdef = mkts.get(nmid, {}) if isinstance(mkts, dict) else {}
            name = mdef.get("name", "?") if isinstance(mdef, dict) else "?"
            discovered_markets[nmid] = name
            print(f"    NEW market {nmid}: {name}")

# ── 3c: sweep ot= values ───────────────────────────────────────────────────────
print(f"\n  Sweeping ot= values...")
candidate_ots = list(range(100, 1000)) + list(range(1000, 3001, 100)) + [None]

for ot in candidate_ots:
    qs = BASE_PARAMS + (f"&ot={ot}" if ot is not None else "")
    body, err = _req_json(session, f"{BASE_URL}?{qs}")
    if body is None:
        if err and err not in ("403", "404", "429"):
            print(f"  ot={str(ot):>5}  error: {err}")
        continue
    mids = _extract_market_ids(body)
    new_mids = mids - set(discovered_markets)
    if new_mids:
        d = body.get("data", body) if isinstance(body, dict) else {}
        mkts = d.get("markets", {}) if isinstance(d, dict) else {}
        for mid in sorted(new_mids):
            mdef = mkts.get(mid, {}) if isinstance(mkts, dict) else {}
            name = mdef.get("name", "?") if isinstance(mdef, dict) else "?"
            discovered_markets[mid] = name
            print(f"  ot={str(ot):>5}  NEW market {mid}: {name}")

# ── Phase 4: match-specific endpoints ────────────────────────────────────────
print(f"\n{'='*70}")
print("Phase 4 — match-specific endpoints")

MATCH_ENDPOINTS = [
    "https://www.oddspedia.com/api/v1/getMatchOdds",
    "https://www.oddspedia.com/api/v1/getMatchMarkets",
    "https://www.oddspedia.com/api/v1/getMatchBettingOdds",
    "https://www.oddspedia.com/api/v1/getMatchInfo",
    "https://www.oddspedia.com/api/v1/getOdds",
    "https://www.oddspedia.com/api/v1/getAmericanOdds",
]

probe_ids = list(dict.fromkeys(
    ([url_match_id] if url_match_id else []) + (match_ids[:4] if match_ids else [])
))

for endpoint in MATCH_ENDPOINTS:
    ep_name = endpoint.split("/")[-1]
    for mid in probe_ids[:3]:
        for params in [
            f"matchId={mid}&language=us&geoCode=US&bookmakerGeoCode=US&bookmakerGeoState=VA",
            f"id={mid}&language=us&geoCode=US",
        ]:
            body, err = _req_json(session, f"{endpoint}?{params}")
            if body is None:
                print(f"  {ep_name} matchId={mid}: {err}")
                continue
            print(f"\n  {ep_name}  matchId={mid}")
            if isinstance(body, dict):
                print(f"    response keys: {list(body.keys())[:10]}")
                if body.get("error") or body.get("status") == "error":
                    print(f"    API error: {body.get('error') or body.get('message', '?')}")
                    continue
                d = body.get("data", body)
                if isinstance(d, dict):
                    print(f"    data keys: {list(d.keys())[:15]}")
                mids_ = _extract_market_ids(body)
                print(f"    markets found: {sorted(mids_)}")
                new_mids = mids_ - set(discovered_markets)
                if new_mids:
                    mkts = d.get("markets", {}) if isinstance(d, dict) else {}
                    for nmid in sorted(new_mids):
                        mdef = mkts.get(nmid, {}) if isinstance(mkts, dict) else {}
                        name = mdef.get("name", "?") if isinstance(mdef, dict) else "?"
                        discovered_markets[nmid] = name
                        print(f"    NEW market {nmid}: {name}")
            break  # stop on first non-None response

# ── Final summary ─────────────────────────────────────────────────────────────
print(f"\n{'='*70}")
print("ALL DISCOVERED MARKET IDs:")
for mid, name in sorted(discovered_markets.items(), key=lambda x: int(x[0]) if x[0].isdigit() else 9999):
    tag = " ← NEW" if mid not in KNOWN_MARKETS else ""
    print(f"  {mid:>5}  {name}{tag}")

print(f"\nAll match-page market IDs: {sorted(all_match_market_ids)}")
print(f"\nSet/game-betting candidates:")
SET_KEYWORDS = ("set", "1st", "2nd", "3rd", "first", "second", "third", "game",
                "correct", "score", "total games", "handicap")
for mid, name in sorted(discovered_markets.items(), key=lambda x: int(x[0]) if x[0].isdigit() else 9999):
    if any(kw in name.lower() for kw in SET_KEYWORDS):
        print(f"  {mid}: {name}")

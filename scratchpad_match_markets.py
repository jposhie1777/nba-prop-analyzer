"""
Discover all Oddspedia tennis odds endpoints and market IDs, including set betting.

Four-phase approach:
  Phase 1 – Listing page: get match URLs + match IDs from __NUXT__.
  Phase 2 – Match page:   extract data from __NUXT__ (Nuxt SSR serves data
                          server-side, so there are no client XHR calls to
                          intercept).  Also attach a response handler as a
                          fallback in case any lazy-loaded XHR fires.
  Phase 3 – Direct API:  use page.evaluate(fetch) from within the browser
                          page context (same-origin → no CORS) to call
                          getAmericanMaxOddsWithPagination with ot= values
                          100-2000 and map every available market ID.
  Phase 4 – Match API:   probe match-specific endpoints (getMatchOdds,
                          getMatchMarkets, etc.) using the match ID from
                          Phase 1, looking for set-betting markets.

Run this from a Codespace/machine that can reach oddspedia.com.
"""
import json
import re
from urllib.parse import urlparse, parse_qs

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
    """Recursively collect values whose key is in *targets*."""
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
    """Return set of market ID strings from an API response body."""
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


def _fetch_json_in_page(page, url, timeout_ms=15000):
    """Use page.evaluate to fetch a URL as JSON from within the browser context."""
    try:
        result = page.evaluate(
            """async (url) => {
                try {
                    const r = await fetch(url, {
                        headers: {
                            'Accept': 'application/json',
                            'X-Requested-With': 'XMLHttpRequest'
                        },
                        credentials: 'include'
                    });
                    if (!r.ok) return {error: r.status};
                    const text = await r.text();
                    try { return JSON.parse(text); }
                    catch(e) { return {error: 'json_parse', text: text.slice(0, 200)}; }
                } catch(e) {
                    return {error: String(e)};
                }
            }""",
            url,
        )
        if isinstance(result, dict) and "error" in result:
            return None, result["error"]
        return result, None
    except Exception as exc:
        return None, str(exc)


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

    # Extract match IDs for later API probing
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

    # Try to extract match ID from the URL (trailing digits)
    url_match_id = None
    id_match = re.search(r"-(\d{5,})$", urlparse(match_url).path)
    if id_match:
        url_match_id = id_match.group(1)
        if url_match_id not in match_ids:
            match_ids.insert(0, url_match_id)
    print(f"  match ID from URL: {url_match_id}")

    # ── Phase 2: match page — __NUXT__ extraction + XHR fallback ─────────────
    print(f"\n{'='*70}")
    print("Phase 2 — match page (__NUXT__ extraction + XHR fallback)")

    # Attach response handler BEFORE navigation (catches any client-side XHR)
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
    # Use domcontentloaded — networkidle times out on Nuxt pages (background
    # keep-alives / analytics keep the network "busy" indefinitely).
    page.goto(match_url, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(8000)  # let lazy XHR / hydration finish

    # Try clicking tabs / "More markets"
    for click_text in ("More markets", "All markets", "Set betting", "Sets", "Games",
                       "1st Set", "2nd Set", "Correct Score"):
        try:
            page.click(f"text={click_text}", timeout=2000)
            page.wait_for_timeout(2000)
        except Exception:
            pass

    # ── Extract Nuxt 3 SSR payload from <script> tags in page HTML ───────────
    # Nuxt 3 embeds payload as <script type="application/json" id="__NUXT_DATA__">
    # or <script id="__NUXT__" type="application/json">.  window.__NUXT__ is
    # often an empty shim in Nuxt 3.
    nuxt_payloads = page.evaluate("""() => {
        const results = {};
        // Check window globals
        for (const key of ['__NUXT__', '__NUXT_DATA__', '__NUXT_PAYLOAD__',
                           '__nuxt_payload', '__initial_state__']) {
            if (window[key] && typeof window[key] === 'object') {
                results[key] = window[key];
            }
        }
        // Check <script type="application/json"> tags
        document.querySelectorAll('script[type="application/json"]').forEach((s, i) => {
            try { results['script_json_' + (s.id || i)] = JSON.parse(s.textContent); }
            catch(e) {}
        });
        // Check <script id="__NUXT_DATA__"> regardless of type
        const nd = document.getElementById('__NUXT_DATA__') ||
                   document.getElementById('__nuxt_data__');
        if (nd) {
            try { results['NUXT_DATA_tag'] = JSON.parse(nd.textContent); }
            catch(e) { results['NUXT_DATA_tag_raw'] = nd.textContent.slice(0, 500); }
        }
        // Scan all window keys for large objects that might hold odds data
        const oddsKeys = Object.keys(window).filter(k => {
            if (['__NUXT__','location','document','history','performance',
                 'window','self','top','parent','frames'].includes(k)) return false;
            if (!window[k] || typeof window[k] !== 'object') return false;
            try { return JSON.stringify(window[k]).includes('moneyline'); }
            catch(e) { return false; }
        }).slice(0, 5);
        oddsKeys.forEach(k => { results['window_' + k] = window[k]; });
        return results;
    }""")

    json_hits = {url: r for url, r in all_responses.items() if r["json_body"] is not None}
    api_hits  = {url: r for url, r in json_hits.items() if "/api/" in url}

    print(f"  total JSON responses captured: {len(json_hits)}")
    print(f"  API (/api/) responses: {len(api_hits)}")

    all_match_market_ids: set[str] = set()
    for url, r in api_hits.items():
        parsed = urlparse(url)
        mids = _extract_market_ids(r["json_body"])
        all_match_market_ids.update(mids)
        body = r["json_body"]
        d = body.get("data", body) if isinstance(body, dict) else {}
        print(f"\n  {parsed.path}")
        print(f"    query : {parsed.query[:120]}")
        print(f"    status: {r['status']}  markets: {sorted(mids)}")
        if isinstance(d, dict):
            extra = [k for k in d if k not in ("matches", "markets")]
            if extra:
                print(f"    extra keys: {extra[:10]}")

    # Report what we found in SSR payload sources
    print(f"\n  SSR payload sources found: {list(nuxt_payloads.keys())}")
    NUXT_TARGETS = {
        "markets", "odds", "matchOdds", "oddsTypes", "marketTypes", "oddsData",
        "oddsMarkets", "betTypes", "betOffers", "marketGroups",
    }
    for src_key, src_val in nuxt_payloads.items():
        nuxt_found = _walk_for_keys(src_val, NUXT_TARGETS)
        if nuxt_found:
            print(f"  [{src_key}] market keys: {list(nuxt_found.keys())}")
            for k, v in nuxt_found.items():
                if isinstance(v, dict):
                    print(f"    [{k}]: dict keys={list(v.keys())[:20]}")
                    all_match_market_ids.update(str(x) for x in v.keys() if str(x).isdigit())
                elif isinstance(v, list) and v:
                    sample = v[0]
                    print(f"    [{k}]: list len={len(v)}, first={'keys:'+str(list(sample.keys())[:8]) if isinstance(sample, dict) else type(sample).__name__}")
        else:
            if isinstance(src_val, dict):
                print(f"  [{src_key}]: no market keys — top keys: {list(src_val.keys())[:10]}")

    # ── Open a dedicated stable probe page (same origin: www.oddspedia.com) ─────
    # Using the match page for probes risks "execution context destroyed" if any
    # tab-click causes navigation.  A separate page that we never navigate away
    # from is safe.
    probe_page = ctx.new_page()
    probe_page.goto("https://www.oddspedia.com/", wait_until="domcontentloaded", timeout=30000)

    # ── Phase 3: probe ot= values via in-page fetch ───────────────────────────
    print(f"\n{'='*70}")
    print("Phase 3 — probing ot= values via in-page fetch()")

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

    # ── Diagnostic: show what ot=None (no filter) returns first ──────────────
    _diag_url = f"{BASE_URL}?{BASE_PARAMS}"
    _diag_body, _diag_err = _fetch_json_in_page(probe_page, _diag_url)
    if _diag_body is None:
        print(f"  DIAGNOSTIC fetch failed: {_diag_err}")
    else:
        _d = _diag_body.get("data", _diag_body) if isinstance(_diag_body, dict) else {}
        print(f"  DIAGNOSTIC ot=None response keys: {list(_diag_body.keys())[:10]}")
        if isinstance(_d, dict):
            print(f"  DIAGNOSTIC data keys: {list(_d.keys())[:15]}")
            _mkts = _d.get("markets", {})
            print(f"  DIAGNOSTIC markets type={type(_mkts).__name__} keys/len={list(_mkts.keys())[:10] if isinstance(_mkts, dict) else len(_mkts) if isinstance(_mkts, list) else '?'}")
            _matches = _d.get("matches", {})
            if isinstance(_matches, dict):
                first_mid = next(iter(_matches), None)
                if first_mid:
                    print(f"  DIAGNOSTIC first match keys: {list(_matches[first_mid].keys())[:15]}")

    # ── Also probe getAmericanMaxOddsWithPagination WITH matchId ─────────────
    print(f"\n  Probing getAmericanMaxOddsWithPagination with matchId...")
    for probe_mid in (match_ids[:3] if match_ids else []):
        _mu = f"{BASE_URL}?matchId={probe_mid}&geoCode=US&bookmakerGeoCode=US&language=us"
        _mb, _me = _fetch_json_in_page(probe_page, _mu)
        if _mb is None:
            print(f"    matchId={probe_mid}  error: {_me}")
            continue
        _d2 = _mb.get("data", _mb) if isinstance(_mb, dict) else {}
        _mids2 = _extract_market_ids(_mb)
        print(f"    matchId={probe_mid}  markets: {sorted(_mids2)}  data keys: {list(_d2.keys())[:10] if isinstance(_d2, dict) else type(_d2).__name__}")
        new_mids2 = _mids2 - set(discovered_markets)
        if new_mids2:
            _mkts2 = _d2.get("markets", {}) if isinstance(_d2, dict) else {}
            for nmid in sorted(new_mids2):
                _mdef = _mkts2.get(nmid, {}) if isinstance(_mkts2, dict) else {}
                _name = _mdef.get("name", "?") if isinstance(_mdef, dict) else "?"
                discovered_markets[nmid] = _name
                print(f"    NEW market {nmid}: {_name}")

    # Sweep ot= values
    candidate_ots = (
        list(range(100, 1000))         # fine sweep – finds all 3-digit market IDs
        + list(range(1000, 3001, 100)) # coarse sweep for 4-digit IDs
        + [None]
    )

    for ot in candidate_ots:
        qs = BASE_PARAMS + (f"&ot={ot}" if ot is not None else "")
        url = f"{BASE_URL}?{qs}"
        body, err = _fetch_json_in_page(probe_page, url)
        if body is None:
            if err and "403" not in str(err) and "429" not in str(err):
                print(f"  ot={str(ot):>5}  fetch error: {err}")
            continue

        mids = _extract_market_ids(body)
        new_mids = mids - set(discovered_markets)
        if new_mids:
            d = body.get("data", body) if isinstance(body, dict) else {}
            mkt_defs = d.get("markets", {}) if isinstance(d, dict) else {}
            for mid in sorted(new_mids):
                if isinstance(mkt_defs, dict):
                    mdef = mkt_defs.get(mid, mkt_defs.get(int(mid) if mid.isdigit() else mid, {}))
                    name = mdef.get("name", "?") if isinstance(mdef, dict) else "?"
                else:
                    name = "?"
                discovered_markets[mid] = name
                print(f"  ot={str(ot):>5}  NEW market {mid}: {name}")

    # ── Phase 4: probe match-specific API endpoints ───────────────────────────
    print(f"\n{'='*70}")
    print("Phase 4 — probing match-specific API endpoints")

    MATCH_ENDPOINTS = [
        "https://www.oddspedia.com/api/v1/getMatchOdds",
        "https://www.oddspedia.com/api/v1/getMatchMarkets",
        "https://www.oddspedia.com/api/v1/getMatchBettingOdds",
        "https://www.oddspedia.com/api/v1/getMatchStats",
        "https://www.oddspedia.com/api/v1/getMatchInfo",
        "https://www.oddspedia.com/api/v1/getOdds",
        "https://www.oddspedia.com/api/v1/getAmericanOdds",
    ]

    probe_ids = (match_ids[:5] if match_ids else []) + (
        [url_match_id] if url_match_id and url_match_id not in match_ids else []
    )

    for endpoint in MATCH_ENDPOINTS:
        ep_name = endpoint.split("/")[-1]
        for mid in (probe_ids[:3] if probe_ids else ["0"]):
            for params in [
                f"matchId={mid}&language=us&geoCode=US&bookmakerGeoCode=US&bookmakerGeoState=VA",
                f"id={mid}&language=us&geoCode=US",
                f"match_id={mid}&language=us&geoCode=US",
            ]:
                url = f"{endpoint}?{params}"
                body, err = _fetch_json_in_page(probe_page, url)
                if body is None:
                    print(f"  {ep_name} matchId={mid}: fetch failed — {err}")
                    continue
                # Always print what we got (even errors — they reveal structure)
                print(f"\n  {ep_name}  matchId={mid}")
                if isinstance(body, dict):
                    print(f"    response keys: {list(body.keys())[:10]}")
                    if body.get("error") or body.get("status") == "error":
                        print(f"    API error: {body.get('error') or body.get('message','?')}")
                        continue  # try next param format
                    d = body.get("data", body)
                    if isinstance(d, dict):
                        print(f"    data keys: {list(d.keys())[:15]}")
                    mids = _extract_market_ids(body)
                    print(f"    markets found: {sorted(mids)}")
                    new_mids = mids - set(discovered_markets)
                    if new_mids:
                        mkt_defs = d.get("markets", {}) if isinstance(d, dict) else {}
                        for nmid in sorted(new_mids):
                            mdef = mkt_defs.get(nmid, {}) if isinstance(mkt_defs, dict) else {}
                            name = mdef.get("name", "?") if isinstance(mdef, dict) else "?"
                            discovered_markets[nmid] = name
                            print(f"    NEW market {nmid}: {name}")
                else:
                    print(f"    non-dict response type: {type(body).__name__}")
                break  # stop trying param formats once we get a non-None body

    browser.close()

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

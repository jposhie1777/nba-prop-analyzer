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
    page.goto(match_url, wait_until="networkidle", timeout=60000)

    # Try clicking tabs / "More markets"
    for click_text in ("More markets", "All markets", "Set betting", "Sets", "Games"):
        try:
            page.click(f"text={click_text}", timeout=2000)
            page.wait_for_load_state("networkidle", timeout=5000)
        except Exception:
            pass

    try:
        page.wait_for_function("() => !!window.__NUXT__", timeout=10000)
        match_nuxt = page.evaluate("() => window.__NUXT__")
    except Exception:
        match_nuxt = {}

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

    # Extract market info from __NUXT__
    NUXT_TARGETS = {
        "markets", "odds", "matchOdds", "oddsTypes", "marketTypes", "oddsData",
        "oddsMarkets", "betTypes", "betOffers", "marketGroups",
    }
    nuxt_found = _walk_for_keys(match_nuxt, NUXT_TARGETS)
    if nuxt_found:
        print(f"\n  __NUXT__ market-related keys found: {list(nuxt_found.keys())}")
        for k, v in nuxt_found.items():
            if isinstance(v, dict):
                print(f"    nuxt['{k}']: dict keys={list(v.keys())[:20]}")
                all_match_market_ids.update(str(x) for x in v.keys() if str(x).isdigit())
            elif isinstance(v, list) and v:
                sample = v[0]
                print(f"    nuxt['{k}']: list len={len(v)}, first keys={list(sample.keys())[:10] if isinstance(sample, dict) else type(sample).__name__}")
    else:
        print("  __NUXT__: no market-related keys found at depth ≤8")
        # Dump top-level __NUXT__ keys for debugging
        if isinstance(match_nuxt, dict):
            print(f"  __NUXT__ top-level keys: {list(match_nuxt.keys())[:20]}")
            for k, v in match_nuxt.items():
                if isinstance(v, (dict, list)):
                    sz = len(v)
                    print(f"    [{k}]: {'dict' if isinstance(v, dict) else 'list'} len={sz}")

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
    BASE_URL = "https://oddspedia.com/api/v1/getAmericanMaxOddsWithPagination"

    discovered_markets: dict[str, str] = dict(KNOWN_MARKETS)

    # Sweep ot= 100..2000 in steps of 100, then fine-sweep promising ranges
    candidate_ots = (
        list(range(100, 2001, 100))   # coarse sweep
        + list(range(101, 200))        # fine-sweep first decade
        + list(range(201, 300))
        + list(range(301, 400))
        + list(range(401, 500))
        + [None]                       # no ot filter
    )

    for ot in candidate_ots:
        qs = BASE_PARAMS + (f"&ot={ot}" if ot is not None else "")
        url = f"{BASE_URL}?{qs}"
        body, err = _fetch_json_in_page(page, url)
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
        "https://oddspedia.com/api/v1/getMatchOdds",
        "https://oddspedia.com/api/v1/getMatchMarkets",
        "https://oddspedia.com/api/v1/getMatchBettingOdds",
        "https://oddspedia.com/api/v1/getMatchStats",
        "https://oddspedia.com/api/v1/getMatchInfo",
        "https://oddspedia.com/api/v1/getOdds",
        "https://oddspedia.com/api/v1/getAmericanOdds",
    ]

    probe_ids = (match_ids[:5] if match_ids else []) + (
        [url_match_id] if url_match_id and url_match_id not in match_ids else []
    )

    for endpoint in MATCH_ENDPOINTS:
        for mid in (probe_ids if probe_ids else ["0"]):
            for params in [
                f"matchId={mid}&language=us&geoCode=US&bookmakerGeoCode=US",
                f"id={mid}&language=us&geoCode=US",
                f"match_id={mid}&language=us&geoCode=US",
            ]:
                url = f"{endpoint}?{params}"
                body, err = _fetch_json_in_page(page, url)
                if body is None:
                    continue
                if isinstance(body, dict) and body.get("error"):
                    continue
                mids = _extract_market_ids(body)
                new_mids = mids - set(discovered_markets)
                print(f"\n  {endpoint.split('/')[-1]}  matchId={mid}")
                print(f"    markets found: {sorted(mids)}")
                if new_mids:
                    d = body.get("data", body) if isinstance(body, dict) else {}
                    mkt_defs = d.get("markets", {}) if isinstance(d, dict) else {}
                    for nmid in sorted(new_mids):
                        mdef = mkt_defs.get(nmid, {}) if isinstance(mkt_defs, dict) else {}
                        name = mdef.get("name", "?") if isinstance(mdef, dict) else "?"
                        discovered_markets[nmid] = name
                        print(f"    NEW market {nmid}: {name}")
                # Print response shape for debugging
                if isinstance(body, dict):
                    print(f"    response keys: {list(body.keys())[:10]}")
                    d = body.get("data", {})
                    if isinstance(d, dict):
                        print(f"    data keys: {list(d.keys())[:15]}")
                break  # found a working params format

    browser.close()

# ── Final summary ─────────────────────────────────────────────────────────────
print(f"\n{'='*70}")
print("ALL DISCOVERED MARKET IDs:")
for mid, name in sorted(discovered_markets.items(), key=lambda x: int(x[0]) if x[0].isdigit() else 9999):
    tag = " ← NEW" if mid not in KNOWN_MARKETS else ""
    print(f"  {mid:>5}  {name}{tag}")

print(f"\nAll match-page market IDs: {sorted(all_match_market_ids)}")
print(f"\nSet-betting candidates (look for 'Set', 'set', '1st', '2nd', '3rd', 'game'):")
for mid, name in sorted(discovered_markets.items(), key=lambda x: int(x[0]) if x[0].isdigit() else 9999):
    if any(kw in name.lower() for kw in ("set", "1st", "2nd", "3rd", "first", "second", "third", "game")):
        print(f"  {mid}: {name}")

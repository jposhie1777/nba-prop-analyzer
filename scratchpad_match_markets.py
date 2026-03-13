"""
Discover all Oddspedia tennis odds endpoints and market IDs, including set betting.

Three-phase approach:
  Phase 1 – Listing page: get match URLs from __NUXT__.
  Phase 2 – Match page:   capture ALL JSON network responses (no /api/ filter).
                          Also read __NUXT__ from the match page directly.
  Phase 3 – Market probe: call getAmericanMaxOddsWithPagination with different
                          ot= values (100–900) to map every available market ID.

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
# Headless shell path (Codespace / linux x86_64 with playwright pre-installed)
_HEADLESS_SHELL = (
    "/root/.cache/ms-playwright/chromium_headless_shell-1194"
    "/chrome-linux/headless_shell"
)
EXCLUDE_PATH_TOKENS = {"picks", "odds-explained", "predictions", "news", "highlights"}

# Known market IDs (from listing-page captures)
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


def _walk_for_keys(obj, targets, depth=0, max_depth=5):
    """Recursively collect values whose key is in *targets*."""
    if depth > max_depth or not isinstance(obj, (dict, list)):
        return {}
    found = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in targets:
                found[k] = v
            found.update(_walk_for_keys(v, targets, depth + 1, max_depth))
    else:
        for item in obj[:10]:
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
        mids.update(markets.keys())
    matches = d.get("matches", {}) if isinstance(d, dict) else {}
    if isinstance(matches, dict):
        for mdata in matches.values():
            if isinstance(mdata, dict):
                mids.update(mdata.keys())
    return mids


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

    # ── Phase 2: match page — all JSON responses ──────────────────────────────
    print(f"\n{'='*70}")
    print("Phase 2 — match page (capturing ALL JSON responses)")

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
        all_responses[response.url] = {
            "status": response.status,
            "resource_type": response.request.resource_type,
            "json_body": body,
        }

    page.on("response", on_response)
    page.goto(match_url, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(10000)

    try:
        page.wait_for_function("() => !!window.__NUXT__", timeout=10000)
        match_nuxt = page.evaluate("() => window.__NUXT__")
    except Exception:
        match_nuxt = {}

    # Also try clicking "More markets" / other tabs if present
    try:
        page.click("text=More markets", timeout=3000)
        page.wait_for_timeout(3000)
    except Exception:
        pass

    json_hits = {url: r for url, r in all_responses.items() if r["json_body"] is not None}
    api_hits  = {url: r for url, r in json_hits.items() if "/api/" in url}

    print(f"  total JSON responses: {len(json_hits)}")
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

    # Match page __NUXT__ scan
    nuxt_found = _walk_for_keys(
        match_nuxt,
        {"markets", "odds", "matchOdds", "oddsTypes", "marketTypes", "oddsData"},
    )
    if nuxt_found:
        print(f"\n  __NUXT__ market keys found: {list(nuxt_found.keys())}")
        for k, v in nuxt_found.items():
            if isinstance(v, dict):
                print(f"    nuxt['{k}']: dict keys={list(v.keys())[:15]}")
            elif isinstance(v, list) and v:
                print(f"    nuxt['{k}']: list len={len(v)}, first keys={list(v[0].keys())[:10] if isinstance(v[0], dict) else type(v[0]).__name__}")

    # ── Phase 3: probe ot= values to find all market IDs ─────────────────────
    print(f"\n{'='*70}")
    print("Phase 3 — probing ot= values on getAmericanMaxOddsWithPagination")

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

    discovered_markets: dict[str, str] = dict(KNOWN_MARKETS)  # id -> name

    # Candidate ot values: 100-900, plus a sweep for no ot filter
    candidate_ots = list(range(100, 1000, 100)) + list(range(101, 200)) + [None]

    probe_page = ctx.new_page()

    for ot in candidate_ots:
        qs = BASE_PARAMS + (f"&ot={ot}" if ot is not None else "")
        url = f"{BASE_URL}?{qs}"

        probe_result = {"done": False, "body": None}

        def _capture(resp, _url=url, _pr=probe_result):
            if resp.url == _url and resp.status == 200:
                try:
                    _pr["body"] = resp.json()
                except Exception:
                    pass
                _pr["done"] = True

        probe_page.on("response", _capture)
        try:
            probe_page.goto(url, wait_until="domcontentloaded", timeout=20000)
        except Exception:
            probe_page.remove_listener("response", _capture)
            continue
        probe_page.remove_listener("response", _capture)

        body = probe_result.get("body")
        if body is None:
            # Try reading the page text directly
            try:
                raw = probe_page.evaluate("() => document.body.innerText")
                body = json.loads(raw)
            except Exception:
                continue

        mids = _extract_market_ids(body)
        new_mids = mids - set(discovered_markets)
        if new_mids:
            d = body.get("data", body) if isinstance(body, dict) else {}
            mkt_defs = d.get("markets", {}) if isinstance(d, dict) else {}
            for mid in new_mids:
                name = mkt_defs.get(mid, {}).get("name", "?") if isinstance(mkt_defs, dict) else "?"
                discovered_markets[mid] = name
                print(f"  ot={ot:>4}  NEW market {mid}: {name}")

    browser.close()

# ── Final summary ─────────────────────────────────────────────────────────────
print(f"\n{'='*70}")
print("ALL DISCOVERED MARKET IDs:")
for mid, name in sorted(discovered_markets.items(), key=lambda x: int(x[0])):
    tag = " ← NEW" if mid not in KNOWN_MARKETS else ""
    print(f"  {mid:>5}  {name}{tag}")

print(f"\nAll match-page market IDs: {sorted(all_match_market_ids)}")
print(f"\nSet-betting candidates (look for 'Set', 'set', '1st', '2nd', '3rd'):")
for mid, name in sorted(discovered_markets.items(), key=lambda x: int(x[0])):
    if any(kw in name.lower() for kw in ("set", "1st", "2nd", "3rd", "first", "second", "third")):
        print(f"  {mid}: {name}")

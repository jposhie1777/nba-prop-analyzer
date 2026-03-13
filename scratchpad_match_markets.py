"""
Discover all Oddspedia tennis odds endpoints and market IDs, including set betting.

Strategy:
  Phase 1 – Listing page:  load the tennis odds listing, extract match list/IDs.
  Phase 2 – Match page:    navigate to a match page; capture XHR + SSR payload.
  Phase 3 – API sweep:     use Playwright's APIRequestContext (ctx.request.get)
                            which uses the browser's TLS stack + cookies, bypassing
                            both CORS (in-page fetch) and TLS-fingerprint blocking
                            (Python requests).  Sweep ot= values + matchId probes.
  Phase 4 – Match endpoints: same APIRequestContext, probe match-specific APIs.

Run from a Codespace/machine that can reach oddspedia.com.
"""
import json
import re
from urllib.parse import urlparse

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


def _api_get(api_ctx, url):
    """
    GET url via Playwright APIRequestContext (browser TLS stack + context cookies).
    Returns (body_dict_or_None, error_str).
    """
    try:
        resp = api_ctx.get(
            url,
            headers={
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "X-Requested-With": "XMLHttpRequest",
            },
            timeout=15000,
        )
        if resp.status != 200:
            return None, str(resp.status)
        return resp.json(), None
    except Exception as e:
        return None, str(e)


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
    print(f"  match IDs: {match_ids[:5]}")

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
        raise RuntimeError("No match URL found.")
    print(f"  selected match: {match_url}")

    url_match_id = None
    id_m = re.search(r"-(\d{5,})$", urlparse(match_url).path)
    if id_m:
        url_match_id = id_m.group(1)
        if url_match_id not in match_ids:
            match_ids.insert(0, url_match_id)
    print(f"  match ID from URL: {url_match_id}")

    # ── Phase 2: match page ───────────────────────────────────────────────────
    print(f"\n{'='*70}")
    print("Phase 2 — match page (XHR capture + SSR payload)")

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
                "json_body": body,
            }

    page.on("response", on_response)
    page.goto(match_url, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(8000)

    for click_text in ("More markets", "All markets", "Set betting",
                       "1st Set", "2nd Set", "Correct Score"):
        try:
            page.click(f"text={click_text}", timeout=2000)
            page.wait_for_timeout(1500)
        except Exception:
            pass

    nuxt_payloads = page.evaluate("""() => {
        const out = {};
        for (const k of ['__NUXT__','__NUXT_DATA__','__NUXT_PAYLOAD__','__nuxt_payload']) {
            if (window[k] && typeof window[k] === 'object') out[k] = window[k];
        }
        document.querySelectorAll('script[type="application/json"]').forEach((s,i) => {
            try { out['script_' + (s.id||i)] = JSON.parse(s.textContent); } catch(e) {}
        });
        const nd = document.getElementById('__NUXT_DATA__');
        if (nd) { try { out['NUXT_DATA_tag'] = JSON.parse(nd.textContent); } catch(e) {} }
        return out;
    }""")

    json_hits = {u: r for u, r in all_responses.items() if r["json_body"] is not None}
    api_hits  = {u: r for u, r in json_hits.items() if "/api/" in u}
    print(f"  JSON responses: {len(json_hits)}  API: {len(api_hits)}")

    all_match_market_ids: set[str] = set()
    for url, r in api_hits.items():
        parsed = urlparse(url)
        mids = _extract_market_ids(r["json_body"])
        all_match_market_ids.update(mids)
        print(f"  {parsed.path}  markets={sorted(mids)}")

    NUXT_TARGETS = {"markets","odds","matchOdds","oddsTypes","marketTypes",
                    "oddsData","oddsMarkets","betTypes","betOffers","marketGroups"}
    print(f"  SSR sources: {list(nuxt_payloads.keys())}")
    for sk, sv in nuxt_payloads.items():
        nf = _walk_for_keys(sv, NUXT_TARGETS)
        if nf:
            print(f"  [{sk}] found: {list(nf.keys())}")
        elif isinstance(sv, dict):
            print(f"  [{sk}] top keys: {list(sv.keys())[:10]}")

    # ── Phases 3/4: APIRequestContext — browser TLS + context cookies ──────────
    # ctx.request.get() uses Chromium's networking stack (same JA3 fingerprint,
    # HTTP/2, etc.) and automatically includes all cookies from the context.
    # This bypasses both CORS (no same-origin restriction) and Cloudflare
    # TLS-fingerprint detection (which blocks Python requests).
    api_ctx = ctx.request

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

    # ── Phase 3 ───────────────────────────────────────────────────────────────
    print(f"\n{'='*70}")
    print("Phase 3 — API sweep via APIRequestContext (browser TLS + cookies)")

    discovered_markets: dict[str, str] = dict(KNOWN_MARKETS)

    # 3a: diagnostic (no filter)
    diag_body, diag_err = _api_get(api_ctx, f"{BASE_URL}?{BASE_PARAMS}")
    if diag_body is None:
        print(f"  DIAGNOSTIC failed: {diag_err}")
    else:
        d = diag_body.get("data", diag_body) if isinstance(diag_body, dict) else {}
        print(f"  DIAGNOSTIC response keys: {list(diag_body.keys())[:10]}")
        if isinstance(d, dict):
            print(f"  DIAGNOSTIC data keys: {list(d.keys())[:15]}")
            mkts = d.get("markets", {})
            print(f"  DIAGNOSTIC markets: {type(mkts).__name__}  sample={list(mkts.keys())[:10] if isinstance(mkts, dict) else len(mkts) if isinstance(mkts, list) else '?'}")
            ms = d.get("matches", {})
            if isinstance(ms, dict) and ms:
                fm = next(iter(ms))
                print(f"  DIAGNOSTIC first match sub-keys: {list(ms[fm].keys())[:15]}")

    # 3b: matchId probe
    print(f"\n  Probing with matchId...")
    for probe_mid in (match_ids[:3] if match_ids else []):
        url = f"{BASE_URL}?matchId={probe_mid}&geoCode=US&bookmakerGeoCode=US&language=us"
        body, err = _api_get(api_ctx, url)
        if body is None:
            print(f"    matchId={probe_mid}  error: {err}")
            continue
        d = body.get("data", body) if isinstance(body, dict) else {}
        mids = _extract_market_ids(body)
        print(f"    matchId={probe_mid}  markets: {sorted(mids)}  data keys: {list(d.keys())[:10] if isinstance(d,dict) else type(d).__name__}")
        new_mids = mids - set(discovered_markets)
        if new_mids:
            mkts = d.get("markets", {}) if isinstance(d, dict) else {}
            for nmid in sorted(new_mids):
                mdef = mkts.get(nmid, {}) if isinstance(mkts, dict) else {}
                name = mdef.get("name", "?") if isinstance(mdef, dict) else "?"
                discovered_markets[nmid] = name
                print(f"    NEW market {nmid}: {name}")

    # 3c: ot= sweep
    print(f"\n  Sweeping ot= values 100..999 + 1000..3000 (step 100)...")
    candidate_ots = list(range(100, 1000)) + list(range(1000, 3001, 100)) + [None]
    for ot in candidate_ots:
        qs = BASE_PARAMS + (f"&ot={ot}" if ot is not None else "")
        body, err = _api_get(api_ctx, f"{BASE_URL}?{qs}")
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

    # ── Phase 4: match-specific endpoints ─────────────────────────────────────
    print(f"\n{'='*70}")
    print("Phase 4 — match-specific endpoints via APIRequestContext")

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
                body, err = _api_get(api_ctx, f"{endpoint}?{params}")
                if body is None:
                    print(f"  {ep_name} matchId={mid}: {err}")
                    continue
                print(f"\n  {ep_name}  matchId={mid}")
                if isinstance(body, dict):
                    print(f"    response keys: {list(body.keys())[:10]}")
                    if body.get("error") or body.get("status") == "error":
                        print(f"    API error: {body.get('error') or body.get('message','?')}")
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
                else:
                    print(f"    non-dict: {type(body).__name__}")
                break

    browser.close()

# ── Final summary ─────────────────────────────────────────────────────────────
print(f"\n{'='*70}")
print("ALL DISCOVERED MARKET IDs:")
for mid, name in sorted(discovered_markets.items(), key=lambda x: int(x[0]) if x[0].isdigit() else 9999):
    tag = " ← NEW" if mid not in KNOWN_MARKETS else ""
    print(f"  {mid:>5}  {name}{tag}")

print(f"\nAll match-page market IDs: {sorted(all_match_market_ids)}")
print(f"\nSet/game-betting candidates:")
SET_KEYWORDS = ("set", "1st", "2nd", "3rd", "first", "second", "third",
                "game", "correct", "score", "total games", "handicap")
for mid, name in sorted(discovered_markets.items(), key=lambda x: int(x[0]) if x[0].isdigit() else 9999):
    if any(kw in name.lower() for kw in SET_KEYWORDS):
        print(f"  {mid}: {name}")

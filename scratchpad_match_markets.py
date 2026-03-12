"""
Probe Oddspedia tennis pages and print sportsbook market-related API payloads.

Workflow:
1) Load listing page and inspect __NUXT__ for upcoming matches.
2) Resolve a likely *match* URL (avoid editorial pages like /picks).
3) Capture JSON responses from any URL containing '/api/' while loading the match page.
"""
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
EXCLUDE_PATH_TOKENS = {
    "picks",
    "odds-explained",
    "predictions",
    "news",
    "highlights",
}


def _tennis_segments(path: str) -> list[str]:
    raw_parts = [p for p in path.strip("/").split("/") if p]
    if "tennis" in raw_parts:
        tennis_idx = raw_parts.index("tennis")
        return raw_parts[tennis_idx + 1 :]
    return raw_parts


def looks_like_match_path(url: str) -> bool:
    parsed = urlparse(url)
    if "/us/tennis/" not in parsed.path:
        return False
    if "#" in url:
        return False

    segments = _tennis_segments(parsed.path)
    if not segments:
        return False

    # Match pages can be nested below section paths (e.g. /us/tennis/odds/<match-slug>).
    slug_segment = segments[-1]
    if slug_segment in EXCLUDE_PATH_TOKENS:
        return False
    if slug_segment in {"odds", "live", "results", "fixtures"}:
        return False

    # Typical match slugs look like player-a-player-b or include an id suffix.
    has_two_sides = bool(re.search(r"[a-z0-9]+-[a-z0-9]+", slug_segment))
    return has_two_sides and slug_segment.count("-") >= 1


def build_match_candidates_from_data(match_list: list[dict]) -> list[str]:
    candidates = []
    for item in match_list:
        raw_url = item.get("url")
        if isinstance(raw_url, str) and raw_url:
            if raw_url.startswith("http"):
                candidates.append(raw_url)
            else:
                candidates.append(f"https://www.oddspedia.com{raw_url if raw_url.startswith('/') else '/' + raw_url}")

        ht_slug = item.get("ht_slug")
        at_slug = item.get("at_slug")
        event_id = item.get("id") or item.get("match_id")
        if isinstance(ht_slug, str) and isinstance(at_slug, str) and ht_slug and at_slug:
            slug = f"{ht_slug}-{at_slug}"
            if event_id:
                slug = f"{slug}-{event_id}"
            candidates.append(f"https://www.oddspedia.com/us/tennis/{slug}")

    return list(dict.fromkeys(candidates))


with sync_playwright() as pw:
    browser = pw.chromium.launch(headless=True)
    ctx = browser.new_context(user_agent=UA, locale="en-US")
    page = ctx.new_page()
    if stealth:
        stealth(page)

    print(f"Step 1: loading listing page ...\n  -> {LISTING_URL}")
    page.goto(LISTING_URL, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_function("() => !!window.__NUXT__", timeout=20000)

    nuxt = page.evaluate("() => window.__NUXT__")
    data0 = (nuxt.get("data") or [{}])[0] or {}
    match_list = data0.get("matchList", [])
    print(f"  matchList entries: {len(match_list)}")
    if match_list:
        print(f"  matchList keys (sample): {sorted(list(match_list[0].keys()))[:20]}")

    upcoming_slugs = set()
    for m in match_list:
        if m.get("winner") is None and m.get("matchstatus") == 1:
            for k in ("ht_slug", "at_slug", "slug", "url"):
                v = m.get(k)
                if isinstance(v, str) and v:
                    upcoming_slugs.add(v)
    print(f"  upcoming slug hints: {len(upcoming_slugs)}")

    all_links = page.evaluate(
        """
        () => Array.from(document.querySelectorAll('a[href]'))
            .map(a => a.href)
            .filter(h => h.includes('/us/tennis/'))
        """
    )
    all_links = list(dict.fromkeys(all_links))
    print(f"  tennis links found in DOM: {len(all_links)}")

    match_candidates = [u for u in all_links if looks_like_match_path(u)]

    data_candidates = [u for u in build_match_candidates_from_data(match_list) if looks_like_match_path(u)]
    if data_candidates:
        print(f"  match links inferred from __NUXT__: {len(data_candidates)}")

    match_candidates = list(dict.fromkeys(data_candidates + match_candidates))
    print(f"  probable match links: {len(match_candidates)}")
    for lnk in match_candidates[:8]:
        print(f"    {lnk}")

    # Prefer candidate containing known upcoming slugs.
    match_url = None
    for lnk in match_candidates:
        parts = _tennis_segments(urlparse(lnk).path)
        if any(slug and any(slug in p for p in parts) for slug in upcoming_slugs):
            match_url = lnk
            break

    if not match_url and match_candidates:
        match_url = match_candidates[0]

    if not match_url:
        raise RuntimeError("Could not find a likely match URL on listing page.")

    print(f"\n  using match URL: {match_url}")

    api_calls = {}
    api_meta = []

    def on_response(response):
        url = response.url
        if "/api/" not in url:
            return
        meta = f"{response.status} {response.request.resource_type:>10} {url[:150]}"
        api_meta.append(meta)
        if response.status != 200:
            return
        ctype = response.headers.get("content-type", "")
        if "json" not in ctype:
            return
        try:
            api_calls[url] = response.json()
        except Exception:
            pass

    page.on("response", on_response)

    print(f"\nStep 2: loading match page ...\n  -> {match_url}")
    page.goto(match_url, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(10000)

    print(f"  api/* responses captured: {len(api_meta)}")
    for m in api_meta[:25]:
        print(f"    {m}")

    browser.close()

print(f"\n{'=' * 70}")
print(f"Captured {len(api_calls)} successful JSON api responses")
for url, body in api_calls.items():
    parsed = urlparse(url)
    endpoint = parsed.path
    print(f"\n[{endpoint}] {parsed.query[:120]}")
    if isinstance(body, dict):
        d = body.get("data", body)
        if isinstance(d, dict):
            for key in ("markets", "bookies", "odds", "match", "event"):
                if key in d:
                    val = d[key]
                    if isinstance(val, dict):
                        print(f"  {key}: dict({len(val)})")
                    elif isinstance(val, list):
                        print(f"  {key}: list({len(val)})")
                    else:
                        print(f"  {key}: {type(val).__name__}")
            if not any(k in d for k in ("markets", "bookies", "odds", "match", "event")):
                print(f"  keys: {list(d.keys())[:15]}")

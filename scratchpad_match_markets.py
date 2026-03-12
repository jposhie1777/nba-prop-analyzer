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
    "odds",
    "odds-explained",
    "predictions",
    "news",
    "highlights",
}


def looks_like_match_path(url: str) -> bool:
    parsed = urlparse(url)
    if "/us/tennis/" not in parsed.path:
        return False
    if "#" in url:
        return False

    tail = parsed.path.split("/us/tennis/")[-1].strip("/")
    if not tail:
        return False

    first_segment = tail.split("/")[0]
    if first_segment in EXCLUDE_PATH_TOKENS:
        return False

    # Typical match slugs look like player-a-player-b or include an id suffix.
    return bool(re.search(r"[a-z0-9]+-[a-z0-9]+", first_segment)) and first_segment.count("-") >= 2


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
    print(f"  probable match links: {len(match_candidates)}")
    for lnk in match_candidates[:8]:
        print(f"    {lnk}")

    # Prefer candidate containing known upcoming slugs.
    match_url = None
    for lnk in match_candidates:
        if any(slug and slug in lnk for slug in upcoming_slugs):
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

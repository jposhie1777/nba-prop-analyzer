"""
Scrape a tennis match page on Oddspedia and dump all available market IDs.

Step 1: Load the listing page and extract real match URLs from <a> tags.
Step 2: Pick the first upcoming match URL.
Step 3: Intercept all /api/v1/ responses on the match page.
"""
import json
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

with sync_playwright() as pw:
    browser = pw.chromium.launch(headless=True)
    ctx = browser.new_context(user_agent=UA, locale="en-US")
    page = ctx.new_page()
    if stealth:
        stealth(page)

    # ── Step 1: listing page ──────────────────────────────────────────────
    print(f"Step 1: loading listing page …\n  → {LISTING_URL}")
    page.goto(LISTING_URL, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_function("() => !!window.__NUXT__", timeout=15000)

    nuxt = page.evaluate("() => window.__NUXT__")
    data0 = (nuxt.get("data") or [{}])[0] or {}
    match_list = data0.get("matchList", [])
    print(f"  matchList: {len(match_list)} entries")

    # Grab slugs of upcoming matches for filtering
    upcoming_slugs = set()
    for m in match_list:
        if m.get("winner") is None and m.get("matchstatus") == 1:
            upcoming_slugs.update([m.get("ht_slug", ""), m.get("at_slug", "")])
    print(f"  upcoming match slugs: {upcoming_slugs}")

    # Extract real hrefs from the page DOM
    all_links = page.evaluate("""
        () => Array.from(document.querySelectorAll('a[href]'))
                   .map(a => a.href)
                   .filter(h => h.includes('/tennis/') && !h.endsWith('/tennis/odds') && !h.endsWith('/tennis'))
    """)
    all_links = list(dict.fromkeys(all_links))
    print(f"  tennis links found in DOM: {len(all_links)}")
    for lnk in all_links[:8]:
        print(f"    {lnk}")

    # Pick a link that contains an upcoming match slug
    match_url = None
    for lnk in all_links:
        if any(slug and slug in lnk for slug in upcoming_slugs):
            match_url = lnk
            break
    if not match_url and all_links:
        match_url = all_links[0]
    print(f"\n  using match URL: {match_url}")

    # ── Step 2: match detail page with interception ───────────────────────
    api_calls = {}
    all_resp_urls = []

    def on_response(response):
        all_resp_urls.append(f"{response.status} {response.url[:100]}")
        if "/api/v1/" in response.url and response.status == 200:
            try:
                api_calls[response.url] = response.json()
            except Exception:
                pass

    page.on("response", on_response)

    print(f"\nStep 2: loading match page …\n  → {match_url}")
    page.goto(match_url, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(8000)

    print(f"  responses captured: {len(all_resp_urls)}")
    api_urls = [u for u in all_resp_urls if "/api/v1/" in u]
    print(f"  /api/v1/ calls: {len(api_urls)}")
    for u in api_urls:
        print(f"    {u}")

    browser.close()

# ── Results ───────────────────────────────────────────────────────────────
print(f"\n{'='*60}")
print(f"Captured {len(api_calls)} successful /api/v1/ responses:")
for url, body in api_calls.items():
    endpoint = url.split("/api/v1/")[1].split("?")[0]
    params   = url.split("?")[1] if "?" in url else ""
    print(f"\n  [{endpoint}]  {params[:100]}")
    if isinstance(body, dict):
        d = body.get("data", body)
        if isinstance(d, dict):
            markets = d.get("markets", {})
            if markets:
                print(f"  markets ({len(markets)}):")
                print(json.dumps(markets, indent=4))
            else:
                print(f"  keys: {list(d.keys())[:12]}")

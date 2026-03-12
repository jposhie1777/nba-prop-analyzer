"""
Scrape a live tennis match page on Oddspedia and dump all available market IDs.

Step 1: hit the main tennis odds page to get today's match slugs.
Step 2: pick the first upcoming match and intercept its API calls.
Step 3: print every market ID found in any /api/v1/ response.
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


def _scrape_listing(pw):
    ctx = pw.chromium.launch(headless=True).new_context(user_agent=UA, locale="en-US")
    page = ctx.new_page()
    if stealth:
        stealth(page)
    print(f"  → {LISTING_URL}")
    page.goto(LISTING_URL, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_function("() => !!window.__NUXT__", timeout=15000)
    nuxt = page.evaluate("() => window.__NUXT__")
    ctx.browser.close()
    return nuxt


def _intercept_match_page(pw, url):
    """Load match page and capture all /api/v1/ XHR responses."""
    api_calls = {}  # url → parsed JSON body

    browser = pw.chromium.launch(headless=True)
    ctx = browser.new_context(user_agent=UA, locale="en-US")
    page = ctx.new_page()
    if stealth:
        stealth(page)

    def on_response(response):
        if "/api/v1/" in response.url and response.status == 200:
            try:
                body = response.json()
                api_calls[response.url] = body
            except Exception:
                pass

    page.on("response", on_response)

    print(f"  → {url}")
    page.goto(url, wait_until="domcontentloaded", timeout=60000)
    # Wait a few seconds for XHR calls to complete
    page.wait_for_timeout(5000)
    browser.close()

    return api_calls


with sync_playwright() as pw:
    # Step 1: get match list
    print("Step 1: fetching match listing …")
    nuxt = _scrape_listing(pw)
    data0 = (nuxt.get("data") or [{}])[0] or {}
    match_list = data0.get("matchList", [])
    print(f"  found {len(match_list)} matches")

    # Step 2: pick upcoming (winner=None, matchstatus=1)
    match_url = None
    for m in match_list:
        ht, at = m.get("ht_slug"), m.get("at_slug")
        if not ht or not at:
            continue
        if m.get("winner") is None and m.get("matchstatus") == 1:
            match_url = f"https://www.oddspedia.com/us/tennis/{ht}-{at}"
            print(f"  picked: {m.get('ht')} vs {m.get('at')} @ {m.get('md')}")
            break

    if not match_url:
        raise SystemExit("No upcoming matches found.")

    # Step 3: intercept API calls on match page
    print("\nStep 2: loading match page and intercepting API calls …")
    api_calls = _intercept_match_page(pw, match_url)

print(f"\nCaptured {len(api_calls)} API calls:")
for url, body in api_calls.items():
    # Extract just the endpoint name and show markets if present
    endpoint = url.split("/api/v1/")[1].split("?")[0]
    params = url.split("?")[1] if "?" in url else ""
    print(f"\n  [{endpoint}]")
    print(f"  params: {params[:120]}")
    if isinstance(body, dict):
        data = body.get("data", body)
        if isinstance(data, dict):
            markets = data.get("markets", {})
            if markets:
                print(f"  markets: {json.dumps(markets, indent=4)}")
            else:
                print(f"  data keys: {list(data.keys())[:10]}")
        else:
            print(f"  data: list[{len(data)}]" if isinstance(data, list) else f"  data: {type(data)}")

"""Quick script to scrape a match page and dump all available market IDs."""
import json
from playwright.sync_api import sync_playwright

try:
    from playwright_stealth import stealth_sync as stealth
except ImportError:
    stealth = None

URL = "https://www.oddspedia.com/us/tennis/arthur-fils-alexander-zverev"

with sync_playwright() as pw:
    browser = pw.chromium.launch(headless=True)
    ctx = browser.new_context(
        user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        locale="en-US",
    )
    page = ctx.new_page()
    if stealth:
        stealth(page)
    print(f"Fetching {URL} ...")
    page.goto(URL, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_function("() => !!window.__NUXT__", timeout=15000)
    nuxt = page.evaluate("() => window.__NUXT__")
    browser.close()

d = nuxt["data"][0]
print("data[0] keys:", list(d.keys()))
print()

for key in ("odds", "matchOdds", "allOdds"):
    if key in d:
        odds = d[key]
        if isinstance(odds, dict):
            markets = odds.get("markets", {})
            print(f"{key}.markets:", json.dumps(markets, indent=2))

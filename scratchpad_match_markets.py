"""
Scrape a live tennis match page on Oddspedia and dump all available market IDs.

Step 1: hit the main tennis odds page to get today's match slugs.
Step 2: pick the first upcoming/live match and scrape its detail page.
Step 3: print every market ID found in __NUXT__.
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


def _scrape(pw, url):
    ctx = pw.chromium.launch(headless=True).new_context(user_agent=UA, locale="en-US")
    page = ctx.new_page()
    if stealth:
        stealth(page)
    print(f"  → {url}")
    page.goto(url, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_function("() => !!window.__NUXT__", timeout=15000)
    nuxt = page.evaluate("() => window.__NUXT__")
    ctx.browser.close()
    return nuxt


with sync_playwright() as pw:
    # Step 1: get match list from listing page
    print("Step 1: fetching match listing …")
    nuxt = _scrape(pw, LISTING_URL)
    data0 = (nuxt.get("data") or [{}])[0] or {}
    match_list = data0.get("matchList", [])
    print(f"  found {len(match_list)} matches")

    # Debug: show status of first 10 matches
    print("\n  Match statuses (first 10):")
    for m in match_list[:10]:
        print(f"    {m.get('ht')} vs {m.get('at')} | "
              f"inplay={m.get('inplay')} winner={m.get('winner')} "
              f"matchstatus={m.get('matchstatus')} archived={m.get('is_match_archived')}")

    # Step 2: upcoming = winner is None AND matchstatus == 1 (scheduled/prematch)
    match_url = None
    for m in match_list:
        ht, at = m.get("ht_slug"), m.get("at_slug")
        if not ht or not at:
            continue
        if m.get("winner") is None and m.get("matchstatus") == 1:
            match_url = f"https://www.oddspedia.com/us/tennis/{ht}-{at}"
            print(f"\n  picked [upcoming]: {m.get('ht')} vs {m.get('at')} @ {m.get('md')}")
            break

    if not match_url:
        raise SystemExit("No upcoming matches found (winner=None, matchstatus=1).")

    # Step 3: scrape the match detail page
    print("\nStep 2: fetching match detail page …")
    nuxt2 = _scrape(pw, match_url)

data_arr = nuxt2.get("data") or []
print(f"\n__NUXT__.data has {len(data_arr)} entries")

for i, entry in enumerate(data_arr):
    if not isinstance(entry, dict) or not entry:
        continue
    print(f"\n--- data[{i}] keys: {list(entry.keys())}")
    for key in ("odds", "matchOdds", "allOdds", "marketOdds"):
        if key not in entry:
            continue
        odds = entry[key]
        if isinstance(odds, dict):
            markets = odds.get("markets", {})
            print(f"\n  {key}.markets ({len(markets)} total):")
            print(json.dumps(markets, indent=4))

from camoufox.sync_api import Camoufox
import json

url = "https://oddspedia.com/us/soccer/seattle-sounders-fc-san-jose-earthquakes-8076?tab=insights"

captured = {}

def handle_response(response):
    try:
        if "oddspedia.com/api" in response.url:
            print("API call:", response.url)
    except:
        pass

with Camoufox(headless=True, geoip=True) as browser:
    context = browser.new_context(locale="en-US")
    page = context.new_page()
    page.on("response", handle_response)
    page.goto(url, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(3000)
    
    html = page.content()
    print("\nheadToHead in HTML:", "headToHead" in html)
    print("ht_wins in HTML:", "ht_wins" in html)
    
    # Find the __NUXT__ script tag
    import re
    match = re.search(r'<script>window\.__NUXT__=(.*?)</script>', html, re.DOTALL)
    if match:
        print("Found __NUXT__ script tag, length:", len(match.group(1)))
    else:
        print("No __NUXT__ script tag found")
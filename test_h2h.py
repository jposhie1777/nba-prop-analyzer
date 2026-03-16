from camoufox.sync_api import Camoufox
import re

url = "https://oddspedia.com/us/soccer/seattle-sounders-fc-san-jose-earthquakes-8076?tab=insights"

with Camoufox(headless=True, geoip=True) as browser:
    context = browser.new_context(locale="en-US")
    page = context.new_page()
    page.goto(url, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(3000)
    
    html = page.content()
    
    # Print the full __NUXT__ script content so we can see its format
    match = re.search(r'<script>(window\.__NUXT__.*?)</script>', html, re.DOTALL)
    if match:
        script = match.group(1)
        print("First 1000 chars of __NUXT__ script:")
        print(script[:1000])
        print("\n...\n")
        print("Last 500 chars:")
        print(script[-500:])

from camoufox.sync_api import Camoufox
import re, json

url = "https://oddspedia.com/us/soccer/seattle-sounders-fc-san-jose-earthquakes-8076?tab=insights"

with Camoufox(headless=True, geoip=True) as browser:
    context = browser.new_context(locale="en-US")
    page = context.new_page()
    page.goto(url, wait_until="networkidle")
    
    html = page.content()
    print("Status: page loaded")
    print("headToHead in response:", "headToHead" in html)
    print("ht_wins in response:", "ht_wins" in html)
    
    # Try to find and print a snippet around headToHead
    idx = html.find("headToHead")
    if idx != -1:
        print("Snippet:", html[idx:idx+500])
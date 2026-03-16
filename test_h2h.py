from camoufox.sync_api import Camoufox
import json

url = "https://oddspedia.com/us/soccer/seattle-sounders-fc-san-jose-earthquakes-8076?tab=insights"

captured = {}

def handle_response(response):
    try:
        if "getMatchH2H" in response.url or "headToHead" in response.url or "h2h" in response.url.lower():
            print("Found H2H endpoint:", response.url)
            captured["h2h"] = response.json()
        # Cast a wide net - print all XHR/fetch calls
        if "oddspedia.com/api" in response.url:
            print("API call:", response.url)
    except:
        pass

with Camoufox(headless=True, geoip=True) as browser:
    context = browser.new_context(locale="en-US")
    page = context.new_page()
    page.on("response", handle_response)
    page.goto(url, wait_until="networkidle")
    
    print("\nCaptured H2H:", captured.get("h2h"))
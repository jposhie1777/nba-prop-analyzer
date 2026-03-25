from playwright.sync_api import sync_playwright
import json

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    context = browser.new_context()
    page = context.new_page()
    
    captured = {}
    
    def handle_request(request):
        if "api.propfinder.app" in request.url:
            print(f"URL: {request.url}")
            print(f"Headers: {json.dumps(dict(request.headers), indent=2)}")
            captured.update(request.headers)
    
    page.on("request", handle_request)
    
    # Add your cookies/token here after extracting from browser
    # context.add_cookies([{"name": "...", "value": "...", "domain": "propfinder.com"}])
    
    page.goto("https://propfinder.com/mlb/hr-matchups")
    page.wait_for_timeout(5000)
    
    browser.close()

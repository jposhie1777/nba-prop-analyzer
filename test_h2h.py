from camoufox.sync_api import Camoufox
import re, json

url = "https://oddspedia.com/us/soccer/seattle-sounders-fc-san-jose-earthquakes-8076?tab=insights"

with Camoufox(headless=True, geoip=True) as browser:
    context = browser.new_context(locale="en-US")
    page = context.new_page()
    page.goto(url, wait_until="networkidle")
    
    # Try to get the resolved data via JavaScript execution
    h2h = page.evaluate("""() => {
        try {
            const store = window.__nuxt__?._vueInstance?.$store 
                       || window.__nuxt__?.context?.store
                       || Object.values(window.__nuxt__?._vueInstance?.$children || {})
                             .find(c => c.$store)?.$store;
            if (store) return store.state.event.headToHead;
        } catch(e) {}
        return null;
    }""")
    
    print("H2H via store:", h2h)
from camoufox.sync_api import Camoufox

url = "https://oddspedia.com/us/soccer/seattle-sounders-fc-san-jose-earthquakes-8076?tab=insights"

with Camoufox(headless=True, geoip=True) as browser:
    context = browser.new_context(locale="en-US")
    page = context.new_page()
    page.goto(url, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(3000)
    
    h2h = page.evaluate("""() => {
        // Walk the Vue component tree to find the store
        function findStore(el) {
            if (!el) return null;
            if (el.__vue__ && el.__vue__.$store) return el.__vue__.$store;
            for (const child of el.children || []) {
                const found = findStore(child);
                if (found) return found;
            }
            return null;
        }
        
        const store = findStore(document.body);
        if (store) return store.state.event.headToHead;
        return null;
    }""")
    
    print("H2H data:", h2h)
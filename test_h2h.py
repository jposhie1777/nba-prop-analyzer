from camoufox.sync_api import Camoufox
import json

url = "https://oddspedia.com/us/soccer/seattle-sounders-fc-san-jose-earthquakes-8076?tab=insights"

with Camoufox(headless=True, geoip=True) as browser:
    context = browser.new_context(locale="en-US")
    page = context.new_page()
    page.goto(url, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(3000)
    
    result = page.evaluate("""() => {
        // Re-execute the __NUXT__ script
        for (const s of document.scripts) {
            if (s.text && s.text.includes('window.__NUXT__')) {
                eval(s.text);
                break;
            }
        }
        
        if (!window.__NUXT__) return 'no __NUXT__';
        
        // Print top level keys
        console.log('__NUXT__ keys:', Object.keys(window.__NUXT__).join(','));
        
        // Try to find headToHead
        const nuxt = window.__NUXT__;
        
        // Recursively search for headToHead
        function findKey(obj, key, depth=0) {
            if (depth > 5) return null;
            if (!obj || typeof obj !== 'object') return null;
            if (obj[key] !== undefined) return obj[key];
            for (const k of Object.keys(obj)) {
                const found = findKey(obj[k], key, depth+1);
                if (found) return found;
            }
            return null;
        }
        
        const h2h = findKey(nuxt, 'headToHead');
        return h2h;
    }""")
    
    print("H2H data:", json.dumps(result, indent=2))

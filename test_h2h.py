from camoufox.sync_api import Camoufox
import re, json

url = "https://oddspedia.com/us/soccer/seattle-sounders-fc-san-jose-earthquakes-8076?tab=insights"

with Camoufox(headless=True, geoip=True) as browser:
    context = browser.new_context(locale="en-US")
    page = context.new_page()
    page.goto(url, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(3000)
    
    html = page.content()
    
    # The __NUXT__ script has already executed on the page
    # but window.__NUXT__ showed as undefined earlier
    # Let's wait longer and try again
    result = page.evaluate("""() => {
        // Try all possible global locations
        const checks = {
            '__NUXT__': typeof window.__NUXT__,
            '__nuxt__': typeof window.__nuxt__,
            'nuxt': typeof window.nuxt,
            '__nuxtState__': typeof window.__nuxtState__,
        };
        console.log(JSON.stringify(checks));
        
        // Try re-executing the script
        for (const s of document.scripts) {
            if (s.text && s.text.includes('window.__NUXT__')) {
                try {
                    eval(s.text);
                    return 'executed, __NUXT__ type: ' + typeof window.__NUXT__;
                } catch(e) {
                    return 'exec error: ' + e.message;
                }
            }
        }
        return checks;
    }""")
    
    print("Result:", result)

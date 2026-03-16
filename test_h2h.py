from camoufox.sync_api import Camoufox
import re

url = "https://oddspedia.com/us/soccer/seattle-sounders-fc-san-jose-earthquakes-8076?tab=insights"

with Camoufox(headless=True, geoip=True) as browser:
    context = browser.new_context(locale="en-US")
    page = context.new_page()
    page.goto(url, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(3000)
    
    html = page.content()
    
    # Find the __NUXT__ script and evaluate it in the page context
    # to get the resolved data
    result = page.evaluate("""() => {
        // Find script tag containing __NUXT__
        for (const s of document.scripts) {
            if (s.text.includes('window.__NUXT__')) {
                // Execute a modified version that returns the value
                const match = s.text.match(/window\.__NUXT__=(.+)/);
                if (match) {
                    try {
                        const nuxt = eval('(' + match[1] + ')');
                        // Nuxt stores data in nuxt.data or nuxt.state
                        console.log('nuxt keys:', Object.keys(nuxt).join(','));
                        return JSON.stringify(Object.keys(nuxt));
                    } catch(e) {
                        return 'eval error: ' + e.message;
                    }
                }
            }
        }
        return 'not found';
    }""")
    
    print("Result:", result)

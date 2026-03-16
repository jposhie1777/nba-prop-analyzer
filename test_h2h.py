from camoufox.sync_api import Camoufox

url = "https://oddspedia.com/us/soccer/seattle-sounders-fc-san-jose-earthquakes-8076?tab=insights"

with Camoufox(headless=True, geoip=True) as browser:
    context = browser.new_context(locale="en-US")
    page = context.new_page()
    page.goto(url, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(3000)
    
    h2h = page.evaluate("""() => {
        // Find all script tags and look for __NUXT__
        const scripts = document.querySelectorAll('script');
        for (const script of scripts) {
            if (script.textContent.includes('headToHead')) {
                console.log('Found script with headToHead, length:', script.textContent.length);
            }
        }
        
        // Try accessing window.__NUXT__ directly
        console.log('window.__NUXT__:', typeof window.__NUXT__);
        console.log('keys:', window.__NUXT__ ? Object.keys(window.__NUXT__) : 'none');
        
        if (window.__NUXT__) {
            return JSON.stringify(Object.keys(window.__NUXT__));
        }
        return null;
    }""")
    
    # Also grab console output
    print("Result:", h2h)
    
    # Check console messages
    page.on("console", lambda msg: print("Console:", msg.text))
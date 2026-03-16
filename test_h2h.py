from camoufox.sync_api import Camoufox

url = "https://oddspedia.com/us/soccer/seattle-sounders-fc-san-jose-earthquakes-8076?tab=insights"

with Camoufox(headless=True, geoip=True) as browser:
    context = browser.new_context(locale="en-US")
    page = context.new_page()
    page.goto(url, wait_until="networkidle")
    
    h2h = page.evaluate("""() => {
        // Try different ways to access Nuxt/Vue
        const attempts = [
            () => document.querySelector('#__nuxt').__vue__.$store.state.event.headToHead,
            () => document.querySelector('#__layout').__vue__.$store.state.event.headToHead,
            () => document.querySelector('body').__vue__.$store.state.event.headToHead,
            () => Object.values(document.querySelector('#__nuxt').__vue__.$children)
                    .find(c => c.$store)?.$store.state.event.headToHead,
        ];
        
        for (const attempt of attempts) {
            try {
                const result = attempt();
                if (result) return result;
            } catch(e) {}
        }
        return null;
    }""")
    
    print("H2H:", h2h)
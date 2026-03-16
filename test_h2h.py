from camoufox.sync_api import Camoufox

url = "https://oddspedia.com/us/soccer/seattle-sounders-fc-san-jose-earthquakes-8076?tab=insights"

with Camoufox(headless=True, geoip=True) as browser:
    context = browser.new_context(locale="en-US")
    page = context.new_page()
    
    # Set up console listener BEFORE goto
    console_messages = []
    page.on("console", lambda msg: console_messages.append(msg.text))
    
    page.goto(url, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(5000)
    
    result = page.evaluate("""() => {
        const info = {
            nuxt_type: typeof window.__NUXT__,
            nuxt_keys: window.__NUXT__ ? Object.keys(window.__NUXT__) : [],
            has_vue: typeof window.Vue !== 'undefined',
        };
        
        // Try finding vue instance on app element
        const app = document.getElementById('__nuxt') || document.getElementById('app');
        if (app) {
            info.app_found = true;
            info.app_vue_keys = app.__vue__ ? Object.keys(app.__vue__) : [];
        }
        
        return info;
    }""")
    
    print("Result:", result)
    print("\nConsole messages:")
    for msg in console_messages:
        print(" -", msg)

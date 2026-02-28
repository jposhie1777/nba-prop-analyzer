const { chromium } = require('playwright');

(async () => {
  const browser = await chromium.launch({
    headless: true,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-gpu',
      '--headless=new'
    ]
  });

  const context = await browser.newContext({
    userAgent:
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36'
  });

  const page = await context.newPage();

  page.on('response', async (response) => {
    const url = response.url();
    if (url.includes('api') || url.includes('football')) {
      console.log('API:', url);
    }
  });

  await page.goto('https://www.premierleague.com/en/', {
    waitUntil: 'domcontentloaded',
    timeout: 60000
  });

  await page.waitForTimeout(10000);

  await browser.close();
})();
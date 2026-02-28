const { chromium } = require('playwright');

(async () => {
  const browser = await chromium.launch({
    headless: true,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage'
    ]
  });

  const page = await browser.newPage();

  page.on('response', async (response) => {
    const url = response.url();
    if (url.includes('api') || url.includes('football')) {
      console.log('API:', url);
    }
  });

  await page.goto('https://www.premierleague.com/en/', {
    waitUntil: 'networkidle'
  });

  await page.waitForTimeout(8000);

  await browser.close();
})();
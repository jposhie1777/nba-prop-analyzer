const { chromium } = require('playwright');

(async () => {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();

  page.on('response', async (response) => {
    const url = response.url();
    if (url.includes('api')) {
      console.log('API:', url);
    }
  });

  await page.goto('https://www.premierleague.com/en/');
  await page.waitForTimeout(8000);

  await browser.close();
})();
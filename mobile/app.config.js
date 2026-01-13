/** @type {import('expo/config').ExpoConfig} */
const config = {
  name: "Pulse",
  slug: "pulse",
  scheme: "pulse",

  web: {
    bundler: "metro",
    output: "static",
  },

  extra: {
    API_URL: "https://pulse-mobile-api-763243624328.us-central1.run.app",
  },
};

module.exports = config;
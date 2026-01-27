import type { ExpoConfig } from "expo/config";

const config: ExpoConfig = {
  name: "Pulse",
  slug: "pulse",
  scheme: "pulse",

  web: {
    bundler: "metro",
    output: "static",

    // 🔥 THIS IS THE FIX
    experimental: {
      type: "module"
    }
  },

  plugins: ["expo-router"],

  experiments: {
    typedRoutes: true
  },

  extra: {
    API_URL:
      "https://pulse-mobile-api-763243624328.us-central1.run.app"
  }
};

export default config;

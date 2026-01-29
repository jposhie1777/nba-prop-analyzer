// app.config.ts
import type { ExpoConfig } from "expo/config";

const config: ExpoConfig = {
  name: "Pulse",
  slug: "pulse",
  scheme: "pulse",

  android: {
    package: "com.anonymous.pulse",
  },

  web: {
    bundler: "metro",  // Keep metro for SDK 54
    output: "static",
  },

  plugins: ["expo-router"],

  experiments: {
    typedRoutes: true,
  },

  extra: {
    API_URL: "https://pulse-mobile-api-763243624328.us-central1.run.app",
    eas: {
      projectId: "f7f03566-58a4-46dd-acfb-93291bc04752",
    },
  },
};

export default config;
// app.config.ts
import type { ExpoConfig } from "expo/config";

const config: ExpoConfig = {
  name: "Pulse",
  slug: "pulse",
  scheme: "pulse",

  /* ---------------------------------
     ANDROID
  ---------------------------------- */
  android: {
    package: "com.anonymous.pulse",
  },

  /* ---------------------------------
     WEB
  ---------------------------------- */
  web: {
    bundler: "metro",
    output: "static",

    // Required for expo-router + web
    experimental: {
      type: "module",
    },
  },

  /* ---------------------------------
     PLUGINS
  ---------------------------------- */
  plugins: ["expo-router"],

  /* ---------------------------------
     ROUTER / TYPES
  ---------------------------------- */
  experiments: {
    typedRoutes: true,
  },

  /* ---------------------------------
     EXTRA / ENV
     (NO eas.projectId here on purpose)
  ---------------------------------- */
  extra: {
    API_URL:
      "https://pulse-mobile-api-763243624328.us-central1.run.app",
  },
};

export default config;
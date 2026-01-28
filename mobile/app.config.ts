// app.config.ts
import type { ExpoConfig } from "expo/config";

const config: ExpoConfig = {
  name: "Pulse",
  slug: "pulse",
  scheme: "pulse",

  /* ---------------------------------
     ✅ REQUIRED FOR PUSH TOKENS / EAS
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

    // 🔥 REQUIRED FOR expo-router + web
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
  ---------------------------------- */
  extra: {
    API_URL:
      "https://pulse-mobile-api-763243624328.us-central1.run.app",

    // ✅ REQUIRED FOR Expo push tokens
    eas: {
      projectId: "REPLACE_WITH_YOUR_PROJECT_ID",
    },
  },
};

export default config;
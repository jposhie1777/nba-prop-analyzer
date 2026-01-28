// app.config.ts
import type { ExpoConfig } from "expo/config";

const config: ExpoConfig = {
  name: "Pulse",
  slug: "pulse",
  scheme: "pulse",

  /* ============================
     ANDROID
  ============================ */
  android: {
    package: "com.anonymous.pulse",
  },

  /* ============================
     WEB (IMPORTANT)
  ============================ */
  web: {
    bundler: "metro",
    output: "static",
    experimental: {
      type: "module",
    },
  },

  /* ============================
     EXPO EXPERIMENTS
     (CRITICAL FIX)
  ============================ */
  expo: {
    experiments: {
      webStaticRendering: false,
    },
  },

  /* ============================
     ROUTER
  ============================ */
  plugins: ["expo-router"],

  /* ============================
     TYPE SAFETY
  ============================ */
  experiments: {
    typedRoutes: true,
  },

  /* ============================
     ENV / EAS
  ============================ */
  extra: {
    API_URL:
      "https://pulse-mobile-api-763243624328.us-central1.run.app",
    eas: {
      projectId: "f7f03566-58a4-46dd-acfb-93291bc04752",
    },
  },
};

export default config;
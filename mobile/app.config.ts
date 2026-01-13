// appconfig
import "dotenv/config";
import type { ExpoConfig } from "expo/config";

const config: ExpoConfig = {
  name: "Pulse",
  slug: "pulse",
  scheme: "pulse",

  extra: {
    // ✅ Existing values (KEEP)
    API_URL: "https://pulse-mobile-api-763243624328.us-central1.run.app",

    // 🔐 Auth0 (NEW)
    AUTH0_DOMAIN: process.env.EXPO_PUBLIC_AUTH0_DOMAIN,
    AUTH0_CLIENT_ID: process.env.EXPO_PUBLIC_AUTH0_CLIENT_ID,
  },

  plugins: [
    "expo-secure-store",
  ],
};

export default config;
// lib/config.ts
import Constants from "expo-constants";

const EXTRA_API_URL =
  Constants.expoConfig?.extra?.API_URL ??
  // @ts-ignore - legacy manifest fallback
  Constants.manifest?.extra?.API_URL;

export const API_BASE =
  process.env.EXPO_PUBLIC_API_URL ??
  EXTRA_API_URL ??
  "https://mobile-api-763243624328.us-central1.run.app/";

export const USE_MOCK_LIVE_DATA =
  process.env.EXPO_PUBLIC_USE_MOCK_LIVE_DATA === "true";  
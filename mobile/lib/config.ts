// lib/config.ts
export const API_BASE =
  process.env.EXPO_PUBLIC_API_URL ??
  "https://pulse-mobile-api-763243624328.us-central1.run.app";

export const USE_MOCK_LIVE_DATA =
  process.env.EXPO_PUBLIC_USE_MOCK_LIVE_DATA === "true";  
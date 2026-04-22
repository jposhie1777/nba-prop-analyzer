// lib/config.ts
import Constants from "expo-constants";

const EXTRA_API_URL =
  Constants.expoConfig?.extra?.API_URL ??
  Constants.expoConfig?.extra?.EXPO_PUBLIC_API_URL ??
  // @ts-ignore - legacy manifest fallback
  Constants.manifest?.extra?.API_URL ??
  // @ts-ignore - legacy manifest fallback
  Constants.manifest?.extra?.EXPO_PUBLIC_API_URL;

const IS_WEB = typeof window !== "undefined";

export const CLOUD_API_BASE = "https://mobile-api-763243624328.us-central1.run.app";

const DEFAULT_API_BASE = IS_WEB ? "/api" : CLOUD_API_BASE;

function fixKnownApiHostTypos(url: string): string {
  // Backward-compat: some envs used pulse-mobile-api-<project>.run.app,
  // but deployed service host is mobile-api-<project>.run.app.
  return url.replace(
    /https:\/\/pulse-mobile-api-(\d+)\.us-central1\.run\.app/i,
    "https://mobile-api-$1.us-central1.run.app"
  );
}

function normalizeApiBase(url: string): string {
  const trimmed = fixKnownApiHostTypos(url.trim());
  if (!trimmed) return `${DEFAULT_API_BASE}/`;

  // Keep relative URLs (e.g. "/api") for web proxy setups.
  if (trimmed.startsWith("/")) {
    return trimmed.endsWith("/") ? trimmed : `${trimmed}/`;
  }

  return trimmed.endsWith("/") ? trimmed : `${trimmed}/`;
}

// On web, always route through Vercel's /api so edge functions (Neon cache)
// are tried before the Cloud Run fallback. Native builds hit Cloud Run directly.
const rawApiBase = IS_WEB
  ? "/api"
  : (EXTRA_API_URL ?? process.env.EXPO_PUBLIC_API_URL ?? DEFAULT_API_BASE);

export const API_BASE = normalizeApiBase(rawApiBase);

export const USE_MOCK_LIVE_DATA =
  process.env.EXPO_PUBLIC_USE_MOCK_LIVE_DATA === "true";

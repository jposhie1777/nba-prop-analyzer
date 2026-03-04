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

const DEFAULT_API_BASE = IS_WEB
  ? "/api"
  : "https://mobile-api-ib5cx6l6fq-uc.a.run.app";

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

const rawApiBase =
  EXTRA_API_URL ??
  process.env.EXPO_PUBLIC_API_URL ??
  DEFAULT_API_BASE;

export const API_BASE = normalizeApiBase(rawApiBase);

export const USE_MOCK_LIVE_DATA =
  process.env.EXPO_PUBLIC_USE_MOCK_LIVE_DATA === "true";

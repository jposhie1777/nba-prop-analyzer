// lib/api.ts

import { API_BASE } from "./config";
// --------------------------------------------------
// API base
// --------------------------------------------------
const API_BASE =
  "https://pulse-mobile-api-763243624328.us-central1.run.app";

// --------------------------------------------------
// Types
// --------------------------------------------------
export type MobileProp = {
  gameId: number;
  gameDate: string;

  playerId: number;
  player: string;
  team: string;
  opponent: string;
  homeAway: "HOME" | "AWAY";

  market: string;
  marketType: string;

  line: number;
  odds: number;

  // -----------------------
  // BASE METRICS
  // -----------------------
  hitRateL10: number;
  edgePct: number;
  confidence_score: number;

  // -----------------------
  // WINDOW METRICS
  // -----------------------
  avg_l5?: number | null;
  avg_l10?: number | null;
  avg_l20?: number | null;

  hit_rate_l5?: number | null;
  hit_rate_l10?: number | null;
  hit_rate_l20?: number | null;

  clear_1p_pct_l5?: number | null;
  clear_1p_pct_l10?: number | null;
  clear_1p_pct_l20?: number | null;

  clear_2p_pct_l5?: number | null;
  clear_2p_pct_l10?: number | null;
  clear_2p_pct_l20?: number | null;

  avg_margin_l5?: number | null;
  avg_margin_l10?: number | null;
  avg_margin_l20?: number | null;

  bad_miss_pct_l5?: number | null;
  bad_miss_pct_l10?: number | null;
  bad_miss_pct_l20?: number | null;

  pace_l5?: number | null;
  pace_l10?: number | null;
  pace_l20?: number | null;

  usage_l5?: number | null;
  usage_l10?: number | null;
  usage_l20?: number | null;

  // -----------------------
  // STATIC CONTEXT
  // -----------------------
  ts_l10?: number | null;
  pace_delta?: number | null;
  delta_vs_line?: number | null;

  // -----------------------
  // META
  // -----------------------
  matchup?: string;
  bookmaker?: string;
  home_team?: string;
  away_team?: string;

  injuryStatus?: string | null;
  injuryNote?: string | null;

  updatedAt: string;
};

export type FetchPropsResponse = {
  date: string;
  minHitRate: number;
  limit: number;
  offset: number;
  count: number;
  props: MobileProp[];
};

// --------------------------------------------------
// API call
// --------------------------------------------------
export async function fetchProps(params?: {
  gameDate?: string;
  minHitRate?: number;
  limit?: number;
  offset?: number;
}): Promise<FetchPropsResponse> {
  const qs = new URLSearchParams();

  if (params?.gameDate) {
    qs.append("game_date", params.gameDate);
  }

  if (params?.minHitRate !== undefined) {
    qs.append("min_hit_rate", params.minHitRate.toString());
  }

  if (params?.limit !== undefined) {
    qs.append("limit", params.limit.toString());
  }

  if (params?.offset !== undefined) {
    qs.append("offset", params.offset.toString());
  }

  const url = `${API_BASE}/props?${qs.toString()}`;

  // -----------------------------
  // DEBUG LOGGING (CRITICAL)
  // -----------------------------
  console.log("📡 FETCHING:", url);

  let res: Response;

  try {
    res = await fetch(url);
  } catch (err) {
    console.error("❌ NETWORK ERROR:", err);
    throw new Error("Network request failed");
  }

  const text = await res.text();

  console.log("📥 STATUS:", res.status);

  if (!res.ok) {
    throw new Error(`API ${res.status}: ${text}`);
  }

  try {
    return JSON.parse(text) as FetchPropsResponse;
  } catch (err) {
    console.error("❌ JSON PARSE ERROR:", err);
    throw new Error("Invalid JSON response from API");
  }
}

//
// ===================================================================
// 🔽 ADDITIONS BELOW — BACKEND CODE VIEWER (READ-ONLY)
// ===================================================================
//

/**
 * Fetch list of exposed backend files
 * GET /debug/code
 */
export async function fetchBackendFiles(): Promise<string[]> {
  const url = `${API_BASE}/debug/code`;
  console.log("📡 FETCHING BACKEND FILE LIST:", url);

  const res = await fetch(url);
  const text = await res.text();

  if (!res.ok) {
    throw new Error(`Backend files error ${res.status}: ${text}`);
  }

  const json = JSON.parse(text);
  return json.files as string[];
}

/**
 * Fetch contents of a single backend file
 * GET /debug/code/{filename}
 */
export async function fetchBackendFile(
  filename: string
): Promise<string> {
  const url = `${API_BASE}/debug/code/${filename}`;
  console.log("📡 FETCHING BACKEND FILE:", url);

  const res = await fetch(url);
  const text = await res.text();

  if (!res.ok) {
    throw new Error(`Backend file error ${res.status}: ${text}`);
  }

  const json = JSON.parse(text);
  return json.content as string;
}
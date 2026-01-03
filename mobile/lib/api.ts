// lib/api.ts

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

  avgL10?: number | null;
  hitRateL10: number;
  edgePct: number;

  // ✅ ADD THESE (optional for safety)
  matchup?: string;            // "MIN @ MIA"
  bookmaker?: string;          // "fanduel" | "draftkings"
  home_team?: string;          // "MIA"
  away_team?: string;          // "MIN"

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
  console.log("📥 BODY:", text);

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

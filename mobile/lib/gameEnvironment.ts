// lib/gameEnvironment.ts
import { API_BASE } from "./config";

export type BlowoutRisk = {
  level: "high" | "moderate" | "low" | "minimal" | "unknown";
  score: number | null;
  label: string;
};

export type GameEnvironment = {
  game_id: number;
  game_date: string;
  start_time_est: string | null;
  home_team_abbr: string;
  away_team_abbr: string;

  // Vegas lines
  vegas_total: number | null;
  spread_home: number | null;
  home_moneyline: number | null;
  away_moneyline: number | null;

  // Pace
  home_pace: number | null;
  away_pace: number | null;
  combined_pace: number | null;
  home_pace_rank: number | null;
  away_pace_rank: number | null;
  home_pace_label: string;
  away_pace_label: string;

  // Scoring
  home_pts_avg: number | null;
  away_pts_avg: number | null;
  projected_total: number | null;

  // Environment
  environment_tier:
    | "SHOOTOUT"
    | "HIGH"
    | "ABOVE_AVG"
    | "AVERAGE"
    | "BELOW_AVG"
    | "GRIND"
    | "UNKNOWN";
  environment_color: string;

  // Blowout risk
  blowout_risk: BlowoutRisk;

  // Rest & B2B
  home_b2b: boolean;
  away_b2b: boolean;
  home_rest_days: number;
  away_rest_days: number;

  // Impacts
  stat_impacts: string[];
};

export type GameEnvironmentResponse = {
  game_date: string;
  count: number;
  games: GameEnvironment[];
};

export async function fetchGameEnvironment(params?: {
  gameDate?: string;
}): Promise<GameEnvironmentResponse> {
  const qs = new URLSearchParams();

  if (params?.gameDate) {
    qs.append("game_date", params.gameDate);
  }

  const query = qs.toString();
  const url = `${API_BASE}/analytics/game-environment${query ? `?${query}` : ""}`;

  const res = await fetch(url, {
    method: "GET",
    credentials: "omit",
    headers: { "Content-Type": "application/json" },
  });

  const text = await res.text();

  if (!res.ok) {
    throw new Error(`API ${res.status}: ${text}`);
  }

  try {
    return JSON.parse(text) as GameEnvironmentResponse;
  } catch (err) {
    console.error("Invalid JSON from game-environment endpoint", err);
    throw new Error("Invalid JSON response from API");
  }
}

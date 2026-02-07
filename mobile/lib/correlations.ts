// lib/correlations.ts
import { API_BASE } from "./config";

export type PlayerMarket = {
  market: string | null;
  line: number | null;
};

export type Correlation = {
  game_id: number | null;
  team_abbr: string;
  player_a_id: number;
  player_a_name: string;
  player_a_markets: PlayerMarket[];
  player_b_id: number;
  player_b_name: string;
  player_b_markets: PlayerMarket[];
  correlation_metric: string;
  correlation_coefficient: number;
  correlation_strength: "strong" | "moderate" | "weak" | "negligible" | "insufficient";
  direction: "positive" | "negative";
  both_over_rate: number | null;
  shared_games: number;
  relevant_markets: string[];
  insight: string;
};

export type CorrelationGame = {
  game_id: number | null;
  home_team_abbr: string | null;
  away_team_abbr: string | null;
  correlations: Correlation[];
};

export type CorrelationsResponse = {
  game_date: string;
  count: number;
  games: CorrelationGame[];
};

export async function fetchCorrelations(params?: {
  gameDate?: string;
  minGames?: number;
  lookback?: number;
}): Promise<CorrelationsResponse> {
  const qs = new URLSearchParams();

  if (params?.gameDate) {
    qs.append("game_date", params.gameDate);
  }
  if (params?.minGames) {
    qs.append("min_games", String(params.minGames));
  }
  if (params?.lookback) {
    qs.append("lookback", String(params.lookback));
  }

  const query = qs.toString();
  const url = `${API_BASE}/analytics/correlations${query ? `?${query}` : ""}`;

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
    return JSON.parse(text) as CorrelationsResponse;
  } catch (err) {
    console.error("Invalid JSON from correlations endpoint", err);
    throw new Error("Invalid JSON response from API");
  }
}

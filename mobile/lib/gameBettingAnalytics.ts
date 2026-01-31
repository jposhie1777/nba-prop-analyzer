import { API_BASE } from "./config";

export type GameBettingAnalyticsRow = {
  game_id: number;
  game_date: string;
  start_time_est?: string | null;
  status?: string | null;
  is_final?: boolean | null;
  home_team_abbr: string;
  away_team_abbr: string;
  home_score_final?: number | null;
  away_score_final?: number | null;
  home_moneyline?: number | null;
  away_moneyline?: number | null;
  spread_home?: number | null;
  spread_away?: number | null;
  total_line?: number | null;
  home_win_pct_l10?: number | null;
  away_win_pct_l10?: number | null;
  home_ats_pct_l10?: number | null;
  away_ats_pct_l10?: number | null;
  home_over_pct_l10?: number | null;
  away_over_pct_l10?: number | null;
  home_avg_margin_l10?: number | null;
  away_avg_margin_l10?: number | null;
  best_bet_market?: string | null;
  best_bet_side?: string | null;
  best_bet_edge?: number | null;
  best_bet_reason?: string | null;
};

export type GameBettingAnalyticsResponse = {
  game_date?: string | null;
  count: number;
  games: GameBettingAnalyticsRow[];
};

export async function fetchGameBettingAnalytics(params?: {
  gameDate?: string;
  includeFinal?: boolean;
  limit?: number;
}): Promise<GameBettingAnalyticsResponse> {
  const qs = new URLSearchParams();

  if (params?.gameDate) {
    qs.append("game_date", params.gameDate);
  }

  if (params?.includeFinal) {
    qs.append("include_final", "true");
  }

  if (params?.limit) {
    qs.append("limit", params.limit.toString());
  }

  const url = `${API_BASE}/analytics/game-betting?${qs.toString()}`;

  const res = await fetch(url, {
    method: "GET",
    credentials: "omit",
    headers: {
      "Content-Type": "application/json",
    },
  });

  const text = await res.text();

  if (!res.ok) {
    throw new Error(`API ${res.status}: ${text}`);
  }

  try {
    return JSON.parse(text) as GameBettingAnalyticsResponse;
  } catch (err) {
    console.error("Invalid JSON from game betting analytics", err);
    throw new Error("Invalid JSON response from API");
  }
}

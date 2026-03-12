import { useMemo } from "react";

import { useEplQuery } from "@/hooks/epl/useEplQuery";

export type EplBettingAnalyticsRow = {
  ingested_at: string;
  league: string;
  game: string;
  start_time_et?: string;
  home_team?: string | null;
  away_team?: string | null;
  bookmaker: string;
  market: string;
  outcome: string;
  line?: number | null;
  price: number;
  implied_probability?: number | null;
  no_vig_probability?: number | null;
  market_hold?: number | null;
  market_avg_price?: number | null;
  market_min_price?: number | null;
  market_max_price?: number | null;
  market_consensus_fair_probability?: number | null;
  probability_vs_market?: number | null;
  is_best_price?: boolean;
  price_rank?: number;
  model_expected_total_goals?: number | null;
  model_away_win_form_edge?: number | null;
  model_home_win_form_edge?: number | null;
  model_total_line_edge?: number | null;
  model_edge_tier?: "Strong" | "Medium" | "Lean" | null;
  analytics_updated_at?: string;
  // Rolling stats from soccer_data.epl_betting_analytics
  home_l3_goals_pg?: number | null;
  home_l5_goals_pg?: number | null;
  home_l7_goals_pg?: number | null;
  away_l3_goals_pg?: number | null;
  away_l5_goals_pg?: number | null;
  away_l7_goals_pg?: number | null;
  home_l3_goals_allowed_pg?: number | null;
  home_l5_goals_allowed_pg?: number | null;
  home_l7_goals_allowed_pg?: number | null;
  away_l3_goals_allowed_pg?: number | null;
  away_l5_goals_allowed_pg?: number | null;
  away_l7_goals_allowed_pg?: number | null;
  home_l3_corners_pg?: number | null;
  home_l5_corners_pg?: number | null;
  home_l7_corners_pg?: number | null;
  away_l3_corners_pg?: number | null;
  away_l5_corners_pg?: number | null;
  away_l7_corners_pg?: number | null;
  home_l3_win_rate?: number | null;
  home_l5_win_rate?: number | null;
  home_l7_win_rate?: number | null;
  away_l3_win_rate?: number | null;
  away_l5_win_rate?: number | null;
  away_l7_win_rate?: number | null;
};

export type EplBettingAnalyticsResponse = {
  date_et: string;
  row_count: number;
  available_markets: string[];
  available_bookmakers: string[];
  rows: EplBettingAnalyticsRow[];
};

type UseEplBettingAnalyticsArgs = {
  market?: string;
  bookmaker?: string;
  min_edge?: number;
  only_best_price?: boolean;
  limit?: number;
};

export function useEplBettingAnalytics(args: UseEplBettingAnalyticsArgs = {}) {
  const params = useMemo(
    () => ({
      market: args.market,
      bookmaker: args.bookmaker,
      min_edge: args.min_edge,
      only_best_price: args.only_best_price,
      limit: args.limit ?? 250,
    }),
    [args.bookmaker, args.limit, args.market, args.min_edge, args.only_best_price]
  );

  return useEplQuery<EplBettingAnalyticsResponse>("/epl/betting-analytics", params);
}

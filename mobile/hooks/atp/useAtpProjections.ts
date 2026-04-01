import { useMemo } from "react";
import { useEplQuery } from "@/hooks/epl/useEplQuery";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type AtpMoneylineProjection = {
  home_win_prob: number;
  away_win_prob: number;
  home_projected_american: number;
  away_projected_american: number;
  home_fd_american?: number | null;
  home_fd_implied_prob?: number | null;
  home_edge?: number | null;
  home_deep_link?: string | null;
  away_fd_american?: number | null;
  away_fd_implied_prob?: number | null;
  away_edge?: number | null;
  away_deep_link?: string | null;
};

export type AtpTotalEdge = {
  line: number;
  side: "over" | "under";
  odds_american: number;
  odds_decimal?: number | null;
  deep_link?: string | null;
  market_name?: string | null;
  edge_games: number;
  direction: "OVER" | "UNDER";
};

export type AtpTotalGamesProjection = {
  projected_total: number;
  edges: AtpTotalEdge[];
};

export type AtpSetSpreadProjection = {
  straight_sets_prob: number;
  home_straight_sets_prob: number;
  away_straight_sets_prob: number;
  home_minus_1_5_fd_american?: number | null;
  home_minus_1_5_fd_implied?: number | null;
  home_minus_1_5_edge?: number | null;
  home_minus_1_5_deep_link?: string | null;
  away_minus_1_5_fd_american?: number | null;
  away_minus_1_5_fd_implied?: number | null;
  away_minus_1_5_edge?: number | null;
  away_minus_1_5_deep_link?: string | null;
};

export type AtpGameSpreadEdge = {
  player: string;
  line: number;
  edge_games: number;
  odds_american: number;
  deep_link?: string | null;
};

export type AtpGameSpreadProjection = {
  projected_home_spread: number;
  edges: AtpGameSpreadEdge[];
};

export type AtpBestEdge = {
  market: string;
  player: string;
  edge: number;
  label: "Strong Edge" | "Edge" | "Lean";
};

export type AtpMatchProjection = {
  event_id: string;
  event_name: string;
  tournament_name: string;
  surface: string;
  player_home: string;
  player_away: string;
  event_start?: string | null;
  home_rank?: number | null;
  away_rank?: number | null;
  home_form_score?: number | null;
  away_form_score?: number | null;
  home_streak?: number | null;
  away_streak?: number | null;
  moneyline: AtpMoneylineProjection;
  total_games?: AtpTotalGamesProjection | null;
  set_spread?: AtpSetSpreadProjection | null;
  game_spread?: AtpGameSpreadProjection | null;
  best_edge?: AtpBestEdge | null;
};

// ---------------------------------------------------------------------------
// Hooks
// ---------------------------------------------------------------------------

export function useAtpProjections() {
  return useEplQuery<AtpMatchProjection[]>("/atp/projections");
}

export function useAtpProjectionForEvent(eventId?: string | null) {
  const params = useMemo(
    () => (eventId ? { event_id: eventId } : undefined),
    [eventId]
  );
  return useEplQuery<AtpMatchProjection[]>(
    "/atp/projections",
    params,
    Boolean(eventId)
  );
}

import { useMemo } from "react";

import { useEplQuery } from "@/hooks/epl/useEplQuery";

// ─── Types ─────────────────────────────────────────────────────────────────────

export type SoccerGame = {
  event_date: string;
  event_start_ts: string;
  league: "EPL" | "MLS" | string;
  fd_event_id: string;
  game: string;
  home_team: string;
  away_team: string;
  model_expected_total_goals?: number | null;
  model_xg_total?: number | null;
  model_btts_probability?: number | null;
  model_expected_corners?: number | null;
  model_expected_cards?: number | null;
  model_home_win_form_edge?: number | null;
  analytics_updated_at?: string | null;
};

export type SoccerSelection = {
  fd_selection_id: string;
  selection_name: string;
  handicap?: number | null;
  odds_american?: number | null;
  odds_decimal?: number | null;
  implied_probability?: number | null;
  no_vig_probability?: number | null;
  market_hold?: number | null;
  is_best_price?: boolean | null;
  fd_deep_link?: string | null;
  fd_parlay_deep_link?: string | null;
  model_total_line_edge?: number | null;
  model_edge_tier?: "Strong" | "Medium" | "Lean" | null;
};

export type SoccerMarket = {
  market_name: string;
  market_type: string;
  fd_market_id: string;
  selections: SoccerSelection[];
};

export type SoccerFormSide = {
  l5_goals_pg?: number | null;
  l5_goals_allowed_pg?: number | null;
  l5_win_rate?: number | null;
  l5_draw_rate?: number | null;
  l5_btts_rate?: number | null;
  l5_corners_pg?: number | null;
  l5_cards_pg?: number | null;
  season_goals_pg?: number | null;
  season_win_rate?: number | null;
};

export type SoccerModelSignals = {
  expected_total_goals?: number | null;
  xg_total?: number | null;
  btts_probability?: number | null;
  expected_corners?: number | null;
  expected_cards?: number | null;
  home_win_form_edge?: number | null;
};

export type SoccerAnalytics = {
  fd_event_id: string;
  game: string;
  league: string;
  home_team: string;
  away_team: string;
  event_start_ts: string;
  model: SoccerModelSignals;
  form: {
    home: SoccerFormSide;
    away: SoccerFormSide;
  };
  markets: SoccerMarket[];
};

// ─── Parlay helpers ────────────────────────────────────────────────────────────

export type ParlayLeg = {
  fd_market_id: string;
  fd_selection_id: string;
  selection_name: string;
  market_name: string;
  odds_american?: number | null;
};

export function buildParlayLink(legs: ParlayLeg[]): string {
  const base = "fanduelsportsbook://launch?deepLink=addToBetslip%3F";
  const params = legs
    .map(
      (leg) =>
        `marketId%5B%5D=${leg.fd_market_id}&selectionId%5B%5D=${leg.fd_selection_id}`
    )
    .join("&");
  return `${base}${params}`;
}

// ─── Hooks ─────────────────────────────────────────────────────────────────────

export function useSoccerGames() {
  return useEplQuery<SoccerGame[]>("/soccer/games");
}

export function useSoccerAnalytics(
  league: string | null | undefined,
  eventId: string | null | undefined
) {
  const params = useMemo(
    () =>
      league && eventId
        ? { league, event_id: eventId }
        : undefined,
    [league, eventId]
  );
  return useEplQuery<SoccerAnalytics>(
    "/soccer/analytics",
    params,
    Boolean(league && eventId)
  );
}

import { useMemo } from "react";

import { useEplQuery } from "@/hooks/epl/useEplQuery";

export type EplOdd = {
  market: string;
  outcome_name: string;
  line_value?: string | null;
  odds_american?: number | null;
  odds_decimal?: number | null;
};

export type EplAdditionalOddsGroup = {
  market: string;
  outcomes: Array<{
    outcome_name: string;
    line_value?: string | null;
    odds_american?: number | null;
    odds_decimal?: number | null;
  }>;
};

export type EplOddsMatch = {
  match_id: number;
  match_key?: number | null;
  date_utc?: string | null;
  home_team: string;
  away_team: string;
  home_logo?: string | null;
  away_logo?: string | null;
  main_odds: {
    h2h: EplOdd[];
    spreads: EplOdd[];
    totals: EplOdd[];
  };
};

export type EplOddsMatchDetail = {
  match: EplOddsMatch & {
    additional_odds: EplAdditionalOddsGroup[];
  };
  head_to_head: Array<{
    ht_wins?: number | null;
    at_wins?: number | null;
    draws?: number | null;
    played_matches?: number | null;
    h2h_starttime?: string | null;
    h2h_ht?: string | null;
    h2h_at?: string | null;
    h2h_hscore?: number | null;
    h2h_ascore?: number | null;
    h2h_league?: string | null;
  }>;
  last_matches: Array<{
    side?: string | null;
    lm_date?: string | null;
    lm_ht?: string | null;
    lm_at?: string | null;
    lm_hscore?: number | null;
    lm_ascore?: number | null;
    lm_outcome?: string | null;
    lm_home?: boolean | null;
    lm_league_id?: number | null;
    lm_matchstatus?: number | null;
  }>;
  betting_stats: Array<{
    category?: string | null;
    sub_tab?: string | null;
    label?: string | null;
    value?: string | null;
    home?: number | null;
    away?: number | null;
    total_matches_home?: number | null;
    total_matches_away?: number | null;
  }>;
  betting_trends: Array<{
    rank?: number | null;
    statement?: string | null;
    teams_json?: unknown;
  }>;
};

export function useEplOddspediaMatches(limit = 100) {
  const params = useMemo(() => ({ limit }), [limit]);
  return useEplQuery<EplOddsMatch[]>("/epl/oddspedia/matches", params);
}

export function useEplOddspediaMatchDetail(matchId?: number | null) {
  return useEplQuery<EplOddsMatchDetail>(
    `/epl/oddspedia/matches/${matchId ?? 0}`,
    undefined,
    Boolean(matchId)
  );
}

import { useMemo } from "react";

import { useEplQuery } from "@/hooks/epl/useEplQuery";

export type AtpOddsPick = {
  bookie?: string | null;
  odds_decimal?: number | null;
  odds_american?: number | null;
};

export type AtpOddsSummary = {
  home?: AtpOddsPick | null;
  away?: AtpOddsPick | null;
  updated_at?: string | null;
} | null;

export type AtpOddsBoardRow = {
  market_group?: string | null;
  market?: string | null;
  period_id?: number | null;
  period_name?: string | null;
  line_value?: string | null;
  outcome_name?: string | null;
  outcome_side?: string | null;
  outcome_order?: number | null;
  bookie?: string | null;
  odds_decimal?: number | null;
  odds_american?: number | null;
  ingested_at?: string | null;
};

export type AtpUpcomingMatch = {
  match_id: number;
  home_team?: string | null;
  away_team?: string | null;
  matchup?: string | null;
  tournament_name?: string | null;
  round_name?: string | null;
  start_time_utc?: string | null;
  match_date_utc?: string | null;
  home_rank?: string | null;
  away_rank?: string | null;
  home_headshot_url?: string | null;
  away_headshot_url?: string | null;
  odds_summary?: AtpOddsSummary;
};

export type AtpMatchupHeader = {
  home_team?: string | null;
  away_team?: string | null;
  matchup?: string | null;
  start_time_utc?: string | null;
  round_name?: string | null;
  tournament_name?: string | null;
  home_rank?: string | null;
  away_rank?: string | null;
  home_headshot_url?: string | null;
  away_headshot_url?: string | null;
};

export type AtpMatchupDetail = {
  match_id: number;
  matchup: AtpMatchupHeader;
  match_info: Record<string, unknown> | null;
  match_keys: {
    rank?: number | null;
    statement?: string | null;
    round_name?: string | null;
  }[];
  betting_info: {
    category?: string | null;
    sub_tab?: string | null;
    label?: string | null;
    value?: string | null;
    home?: number | null;
    away?: number | null;
    total_matches_home?: number | null;
    total_matches_away?: number | null;
  }[];
  head_to_head_summary: {
    ht_wins?: number | null;
    at_wins?: number | null;
    draws?: number | null;
    played_matches?: number | null;
    period_years?: string | null;
  } | null;
  head_to_head_stats: {
    h2h_starttime?: string | null;
    h2h_ht?: string | null;
    h2h_at?: string | null;
    h2h_hscore?: number | null;
    h2h_ascore?: number | null;
    h2h_winner?: string | null;
    h2h_league_name?: string | null;
  }[];
  head_to_head_matches: {
    h2h_starttime?: string | null;
    h2h_ht?: string | null;
    h2h_at?: string | null;
    h2h_hscore?: number | null;
    h2h_ascore?: number | null;
    h2h_winner?: string | null;
    h2h_league_name?: string | null;
  }[];
  recent_matches: {
    side?: string | null;
    last_starttime?: string | null;
    last_ht?: string | null;
    last_at?: string | null;
    last_hscore?: number | null;
    last_ascore?: number | null;
    last_outcome?: string | null;
  }[];
  odds_summary?: AtpOddsSummary;
  odds_board: AtpOddsBoardRow[];
  odds_updated_at?: string | null;
};

export function useAtpUpcomingMatches(limit = 100, lookaheadDays = 14) {
  const params = useMemo(
    () => ({ limit, lookahead_days: lookaheadDays }),
    [limit, lookaheadDays]
  );
  return useEplQuery<AtpUpcomingMatch[]>("/atp/matchups/upcoming", params);
}

export function useAtpMatchupDetail(matchId?: number | null) {
  return useEplQuery<AtpMatchupDetail>(
    `/atp/matchups/${matchId ?? 0}`,
    undefined,
    Boolean(matchId)
  );
}

import { useMemo } from "react";

import { useEplQuery } from "@/hooks/epl/useEplQuery";

export type SoccerLeague = "epl" | "mls";

export type SoccerUpcomingMatch = {
  match_id: number;
  home_team?: string | null;
  away_team?: string | null;
  matchup?: string | null;
  start_time_utc?: string | null;
  home_recent_form?: string | null;
  away_recent_form?: string | null;
  home_logo?: string | null;
  away_logo?: string | null;
};

export type SoccerStandingRow = {
  team_name?: string | null;
  win_loss_record?: string | null;
  standing_note?: string | null;
  wins?: number | null;
  losses?: number | null;
  draws?: number | null;
};

export type SoccerMatchDetail = {
  league: SoccerLeague;
  match_id: number;
  match_info: Record<string, unknown> | null;
  match_keys: {
    rank?: number | null;
    statement?: string | null;
    round_name?: string | null;
  }[];
  betting_stats: {
    category?: string | null;
    sub_tab?: string | null;
    label?: string | null;
    value?: string | null;
    home?: number | null;
    away?: number | null;
    total_matches_home?: number | null;
    total_matches_away?: number | null;
  }[];
  last_matches: {
    side?: string | null;
    lm_date?: string | null;
    lm_ht?: string | null;
    lm_at?: string | null;
    lm_hscore?: number | null;
    lm_ascore?: number | null;
    lm_outcome?: string | null;
  }[];
};

export function useSoccerUpcomingMatches(league: SoccerLeague, limit = 100, lookaheadDays = 14) {
  const params = useMemo(
    () => ({ limit, lookahead_days: lookaheadDays }),
    [limit, lookaheadDays]
  );
  return useEplQuery<SoccerUpcomingMatch[]>(`/${league}/matchups/upcoming`, params);
}

export function useSoccerMatchupDetail(league: SoccerLeague, matchId?: number | null) {
  return useEplQuery<SoccerMatchDetail>(
    `/${league}/matchups/${matchId ?? 0}`,
    undefined,
    Boolean(matchId)
  );
}

export function useSoccerStandings(league: SoccerLeague) {
  return useEplQuery<SoccerStandingRow[]>(`/${league}/standings`);
}

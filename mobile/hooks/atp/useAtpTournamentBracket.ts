import { useAtpQuery } from "./useAtpQuery";
import type { AtpCompareResponse } from "@/types/atp";

export type AtpBracketMatch = {
  id: number | string | null;
  round: string;
  round_order?: number | null;
  status?: string | null;
  scheduled_at?: string | null;
  match_date?: string | null;
  not_before_text?: string | null;
  player1: string;
  player2: string;
  player1_id?: number | null;
  player2_id?: number | null;
  player1_headshot_url?: string | null;
  player2_headshot_url?: string | null;
  winner?: string;
  score?: string | null;
};

export type AtpBracketRound = {
  name: string;
  order?: number | null;
  matches: AtpBracketMatch[];
};

export type AtpTournamentBracketResponse = {
  tournament: {
    id: number;
    name: string;
    surface?: string | null;
    start_date?: string | null;
    end_date?: string | null;
    category?: string | null;
    city?: string | null;
    country?: string | null;
  };
  bracket: {
    rounds: AtpBracketRound[];
  };
  upcoming_matches: AtpBracketMatch[];
  match_analyses?: Record<string, AtpCompareResponse>;
  match_count: number;
};

type BracketParams = {
  tournamentName?: string;
  tournamentId?: number;
  season?: number;
  upcomingLimit?: number;
};

export function useAtpTournamentBracket(params: BracketParams = {}) {
  const { tournamentName, tournamentId, season, upcomingLimit } = params;
  return useAtpQuery<AtpTournamentBracketResponse>(
    "/atp/tournament-bracket",
    {
      tournament_name: tournamentName,
      tournament_id: tournamentId,
      season,
      upcoming_limit: upcomingLimit,
    }
  );
}

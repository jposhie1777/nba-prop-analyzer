import { useAtpQuery } from "./useAtpQuery";

export type AtpBracketMatch = {
  id: number | string | null;
  round: string;
  round_order?: number | null;
  status?: string | null;
  scheduled_at?: string | null;
  player1: string;
  player2: string;
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

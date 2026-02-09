import { useAtpQuery } from "./useAtpQuery";

export type ActiveTournament = {
  id: number;
  name: string;
  surface?: string | null;
  start_date?: string | null;
  end_date?: string | null;
  category?: string | null;
  city?: string | null;
  country?: string | null;
};

type ActiveTournamentsResponse = {
  tournaments: ActiveTournament[];
  count: number;
};

export function useAtpActiveTournaments() {
  return useAtpQuery<ActiveTournamentsResponse>("/atp/active-tournaments");
}

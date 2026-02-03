import { usePgaQuery } from "./usePgaQuery";
import { PgaTournament } from "@/types/pga";

type Response = {
  data: PgaTournament[];
  count: number;
};

export function usePgaTournaments(params?: { season?: number; status?: string }) {
  return usePgaQuery<Response>("/pga/tournaments", {
    season: params?.season,
    status: params?.status,
    per_page: 50,
  });
}

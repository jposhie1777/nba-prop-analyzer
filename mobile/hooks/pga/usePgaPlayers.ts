import { usePgaQuery } from "./usePgaQuery";
import { PgaPlayer } from "@/types/pga";

type Response = {
  data: PgaPlayer[];
  count: number;
};

export function usePgaPlayers(params?: { search?: string; active?: boolean }) {
  return usePgaQuery<Response>("/pga/players", {
    search: params?.search,
    active: params?.active,
    per_page: 50,
  });
}

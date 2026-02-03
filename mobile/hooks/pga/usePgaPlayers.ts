import { usePgaQuery } from "./usePgaQuery";
import { PgaPlayer } from "@/types/pga";

type Response = {
  data: PgaPlayer[];
  count: number;
};

export function usePgaPlayers(params?: { search?: string }) {
  return usePgaQuery<Response>("/pga/players", {
    search: params?.search,
    per_page: 50,
  });
}

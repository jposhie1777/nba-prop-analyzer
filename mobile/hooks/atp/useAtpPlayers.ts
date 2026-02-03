import { useAtpQuery } from "./useAtpQuery";
import { AtpPlayer } from "@/types/atp";

type Response = {
  data: AtpPlayer[];
  count: number;
};

export function useAtpPlayers(params?: { search?: string }) {
  return useAtpQuery<Response>("/atp/players", {
    search: params?.search,
    per_page: 50,
  });
}

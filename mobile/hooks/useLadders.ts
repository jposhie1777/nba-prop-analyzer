// hooks/useLadders.ts
import { useQuery } from "@tanstack/react-query";
import { API_BASE } from "@/lib/config";

export type Rung = {
  line: number;
  odds: number;
  ladder_score: number;
};

export type VendorBlock = {
  vendor: string;
  rungs: Rung[];
};

export type GameState = "UPCOMING" | "LIVE" | "FINAL";

export type GameScore = {
  home: number | null;
  away: number | null;
};

export type GameClock = {
  period: string | null;
  clock: string | null;
};

export type Ladder = {
  game_id: number;
  player_id: number;
  player_name: string;
  player_team_abbr: string;
  opponent_team_abbr: string;
  game_state: GameState;
  market: string;
  ladder_tier: string;
  anchor_line: number;
  ladder_score: number;
  ladder_by_vendor: VendorBlock[];
  // Live-only fields
  current_stat?: number | null;
  game_score?: GameScore;
  game_clock?: GameClock;
};

export type LadderMode = "pre-live" | "live";

type UseLaddersOptions = {
  mode?: LadderMode;
  limit?: number;
  minVendors?: number;
  market?: string;
};

export function useLadders(options: UseLaddersOptions = {}) {
  const { mode = "pre-live", limit = 50, minVendors = 1, market } = options;

  const query = useQuery<Ladder[]>({
    queryKey: ["ladders", mode, limit, minVendors, market],
    queryFn: async () => {
      const params = new URLSearchParams();
      params.set("mode", mode);
      params.set("limit", limit.toString());
      params.set("min_vendors", minVendors.toString());
      if (market) {
        params.set("market", market);
      }

      const res = await fetch(`${API_BASE}/ladders?${params.toString()}`);

      if (!res.ok) {
        const text = await res.text();
        throw new Error(text);
      }

      const json = await res.json();
      return json.ladders ?? [];
    },

    refetchInterval: mode === "live" ? 15_000 : 60_000, // Faster refresh for live
    staleTime: mode === "live" ? 10_000 : 30_000,
  });

  return {
    data: query.data ?? [],
    loading: query.isLoading,
    error: query.error,
    refetch: query.refetch,
  };
}

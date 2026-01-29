// hooks/useBadLineAlert
import { useQuery } from "@tanstack/react-query";
import { API_BASE } from "@/lib/config";

export type BadLineAlert = {
  prop_id: number;
  game_id: number;
  player_id: number;
  player_name: string;
  market: string;
  market_window: string;
  line_value: number;
  odds: number;
  odds_side: string;

  expected_stat: number;
  expected_edge: number;
  bad_line_score: number;
  is_bad_line: boolean;

  hit_rate_l5: number;
  hit_rate_l10: number;
  hit_rate_l20: number;

  home_team_abbr: string;
  away_team_abbr: string;
};

export function useBadLineAlerts(minScore = 1.0) {
  return useQuery<BadLineAlert[]>({
    queryKey: ["bad-line-alerts", minScore],
    queryFn: async () => {
      const res = await fetch(
        `${API_BASE}/alerts/bad-lines?min_score=${minScore}`
      );

      if (!res.ok) {
        const text = await res.text();
        throw new Error(text);
      }

      const json = await res.json();
      return json.bad_lines ?? [];
    },

    refetchInterval: 30_000, // live-ish
    staleTime: 15_000,
  });
}
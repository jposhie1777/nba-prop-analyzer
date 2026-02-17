import { useAtpQuery } from "./useAtpQuery";
import type { AtpBettingAnalyticsResponse } from "@/types/atp";

type BettingAnalyticsParams = {
  playerIds: number[];
  surface?: string | null;
};

export function useAtpBettingAnalytics(params: BettingAnalyticsParams) {
  const { playerIds, surface } = params;
  const enabled = playerIds.length > 0;

  return useAtpQuery<AtpBettingAnalyticsResponse>(
    "/atp/analytics/betting",
    {
      player_ids: enabled ? playerIds : [],
      surface: surface?.toLowerCase() || undefined,
    },
    enabled
  );
}

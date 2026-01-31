import { useCallback, useEffect, useMemo, useState } from "react";

import {
  fetchGameBettingAnalytics,
  GameBettingAnalyticsResponse,
} from "@/lib/gameBettingAnalytics";

const formatDate = (date: Date) => date.toISOString().slice(0, 10);

export function useGameBettingAnalytics() {
  const [data, setData] = useState<GameBettingAnalyticsResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const gameDate = useMemo(() => formatDate(new Date()), []);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await fetchGameBettingAnalytics({
        gameDate,
        includeFinal: false,
        limit: 40,
      });
      setData(response);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unknown error");
    } finally {
      setLoading(false);
    }
  }, [gameDate]);

  useEffect(() => {
    load();
  }, [load]);

  return {
    data,
    loading,
    error,
    refresh: load,
  };
}

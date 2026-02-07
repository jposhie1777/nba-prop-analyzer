// hooks/useGameEnvironment.ts
import { useCallback, useEffect, useState } from "react";

import {
  fetchGameEnvironment,
  GameEnvironmentResponse,
} from "@/lib/gameEnvironment";

type Result = {
  data: GameEnvironmentResponse | null;
  loading: boolean;
  error: string | null;
  refresh: () => void;
};

export function useGameEnvironment(gameDate?: string): Result {
  const [data, setData] = useState<GameEnvironmentResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await fetchGameEnvironment({ gameDate });
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

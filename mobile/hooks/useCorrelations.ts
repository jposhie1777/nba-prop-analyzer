// hooks/useCorrelations.ts
import { useCallback, useEffect, useState } from "react";

import {
  fetchCorrelations,
  CorrelationsResponse,
} from "@/lib/correlations";

type Result = {
  data: CorrelationsResponse | null;
  loading: boolean;
  error: string | null;
  refresh: () => void;
};

export function useCorrelations(gameDate?: string): Result {
  const [data, setData] = useState<CorrelationsResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await fetchCorrelations({ gameDate });
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

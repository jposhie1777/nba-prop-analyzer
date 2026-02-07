// hooks/useSharpMoves.ts
import { useCallback, useEffect, useState } from "react";

import { fetchSharpMoves, SharpMovesResponse } from "@/lib/sharpMoves";

type Result = {
  data: SharpMovesResponse | null;
  loading: boolean;
  error: string | null;
  refresh: () => void;
};

export function useSharpMoves(gameDate?: string): Result {
  const [data, setData] = useState<SharpMovesResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await fetchSharpMoves({ gameDate });
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

import { useCallback, useEffect, useState } from "react";

import {
  fetchThreeQ100,
  ThreeQ100Response,
} from "@/lib/threeQuarter100";

type Result = {
  data: ThreeQ100Response | null;
  loading: boolean;
  error: string | null;
  refresh: () => void;
};

export function useThreeQuarter100(gameDate?: string): Result {
  const [data, setData] = useState<ThreeQ100Response | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(
    async (forceRefresh = false) => {
      setLoading(true);
      setError(null);
      try {
        const response = await fetchThreeQ100({
          gameDate,
          refresh: forceRefresh,
        });
        setData(response);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Unknown error");
      } finally {
        setLoading(false);
      }
    },
    [gameDate]
  );

  useEffect(() => {
    load(false);
  }, [load]);

  return {
    data,
    loading,
    error,
    refresh: () => load(true),
  };
}

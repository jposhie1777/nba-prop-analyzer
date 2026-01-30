import { useCallback, useEffect, useState } from "react";
import { fetchPlayerPositions } from "@/lib/apiMaster";

export type PlayerPositionRow = {
  player_id: number;
  position: string;
};

export function usePlayerPositions() {
  const [data, setData] = useState<PlayerPositionRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const fetchData = useCallback(async () => {
    try {
      setLoading(true);
      setError(null);
      const rows = await fetchPlayerPositions();
      setData(rows);
    } catch (err: any) {
      setError(err.message ?? "Unknown error");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  return {
    data,
    loading,
    error,
    refetch: fetchData,
  };
}

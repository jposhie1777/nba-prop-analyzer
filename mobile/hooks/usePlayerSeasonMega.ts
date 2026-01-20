import { useEffect, useState, useCallback } from "react";
import Constants from "expo-constants";

const API = Constants.expoConfig?.extra?.API_URL!;

export type PlayerSeasonMegaRow = {
  player_id: number;
  season: number;
  season_type: string;
  first_name: string;
  last_name: string;
  position: string;
  age: number;
  [key: string]: any; // wide table
};

type Result = {
  rows: PlayerSeasonMegaRow[];
  count: number;
  loading: boolean;
  error: string | null;
  refetch: () => void;
};

export function usePlayerSeasonMega(
  opts?: {
    limit?: number;
    enabled?: boolean;
  }
): Result {
  const limit = opts?.limit ?? 500;
  const enabled = opts?.enabled ?? true;

  const [rows, setRows] = useState<PlayerSeasonMegaRow[]>([]);
  const [count, setCount] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetchData = useCallback(async () => {
    if (!enabled) return;

    setLoading(true);
    setError(null);

    try {
      const res = await fetch(
        `${API}/players/season-mega?limit=${limit}`
      );

      if (!res.ok) {
        throw new Error(`HTTP ${res.status}`);
      }

      const json = await res.json();

      setRows(json.rows ?? []);
      setCount(json.count ?? 0);
    } catch (err: any) {
      console.error("[usePlayerSeasonMega] failed", err);
      setError(err.message ?? "Unknown error");
    } finally {
      setLoading(false);
    }
  }, [limit, enabled]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  return {
    rows,
    count,
    loading,
    error,
    refetch: fetchData,
  };
}
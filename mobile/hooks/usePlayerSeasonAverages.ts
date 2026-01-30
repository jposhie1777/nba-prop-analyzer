// hooks/usePlayerSeasonAverages.ts
import { useEffect, useState, useCallback } from "react";

const API = process.env.EXPO_PUBLIC_API_URL!;

export type PlayerSeasonAveragesRow = {
  player_id: number;
  player_first_name: string;
  player_last_name: string;
  player_position: string;
  player_height: string;
  player_weight: string;
  player_jersey_number: string;
  player_college: string;
  player_country: string;
  season: number;
  season_type: string;
  gp: number;
  min: number;
  pts: number;
  reb: number;
  ast: number;
  stl: number;
  blk: number;
  tov: number;
  pf: number;
  oreb: number;
  dreb: number;
  fga: number;
  fgm: number;
  fg_pct: number;
  fg3a: number;
  fg3m: number;
  fg3_pct: number;
  fta: number;
  ftm: number;
  ft_pct: number;
  dd2: number;
  td3: number;
  plus_minus: number;
  nba_fantasy_pts: number;
  w_pct: number;
  pts_rank: number;
  reb_rank: number;
  ast_rank: number;
  stl_rank: number;
  blk_rank: number;
  [key: string]: any;
};

type Result = {
  rows: PlayerSeasonAveragesRow[];
  count: number;
  loading: boolean;
  error: string | null;
  refetch: () => void;
};

export function usePlayerSeasonAverages(opts?: {
  season?: number;
  seasonType?: string;
  search?: string;
  limit?: number;
  enabled?: boolean;
}): Result {
  const season = opts?.season ?? 2025;
  const seasonType = opts?.seasonType ?? "regular";
  const search = opts?.search ?? "";
  const limit = opts?.limit ?? 500;
  const enabled = opts?.enabled ?? true;

  const [rows, setRows] = useState<PlayerSeasonAveragesRow[]>([]);
  const [count, setCount] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetchData = useCallback(async () => {
    if (!enabled) return;

    setLoading(true);
    setError(null);

    try {
      const params = new URLSearchParams({
        season: String(season),
        season_type: seasonType,
        limit: String(limit),
      });

      if (search) {
        params.append("search", search);
      }

      const res = await fetch(
        `${API}/season-averages/players?${params}`,
        { credentials: "omit" }
      );

      if (!res.ok) {
        const text = await res.text();
        throw new Error(text || `HTTP ${res.status}`);
      }

      const json = await res.json();

      setRows(json.rows ?? []);
      setCount(json.count ?? 0);
    } catch (err: any) {
      console.error("[usePlayerSeasonAverages] failed", err);
      setError(err?.message ?? "Unknown error");
    } finally {
      setLoading(false);
    }
  }, [season, seasonType, search, limit, enabled]);

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

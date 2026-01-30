// hooks/useTeamSeasonAverages.ts
import { useEffect, useState, useCallback } from "react";

const API = process.env.EXPO_PUBLIC_API_URL!;

export type TeamSeasonAveragesRow = {
  team_id: number;
  team_conference: string;
  team_division: string;
  team_city: string;
  team_name: string;
  team_full_name: string;
  team_abbreviation: string;
  season: number;
  season_type: string;
  gp: number;
  w: number;
  l: number;
  w_pct: number;
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
  plus_minus: number;
  pts_rank: number;
  reb_rank: number;
  ast_rank: number;
  w_rank: number;
  l_rank: number;
  [key: string]: any;
};

type Result = {
  rows: TeamSeasonAveragesRow[];
  count: number;
  loading: boolean;
  error: string | null;
  refetch: () => void;
};

export function useTeamSeasonAverages(opts?: {
  season?: number;
  seasonType?: string;
  search?: string;
  limit?: number;
  enabled?: boolean;
}): Result {
  const season = opts?.season ?? 2025;
  const seasonType = opts?.seasonType ?? "regular";
  const search = opts?.search ?? "";
  const limit = opts?.limit ?? 30;
  const enabled = opts?.enabled ?? true;

  const [rows, setRows] = useState<TeamSeasonAveragesRow[]>([]);
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
        `${API}/season-averages/teams?${params}`,
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
      console.error("[useTeamSeasonAverages] failed", err);
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

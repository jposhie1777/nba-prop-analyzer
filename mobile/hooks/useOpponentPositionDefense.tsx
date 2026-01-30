import { useCallback, useEffect, useState } from "react";
import Constants from "expo-constants";

const API = Constants.expoConfig?.extra?.API_URL!;

export type OpponentPositionDefenseRow = {
  opponent_team_abbr: string;
  player_position: string;
  games_played: number;
  pts_allowed_avg: number;
  reb_allowed_avg: number;
  ast_allowed_avg: number;
  stl_allowed_avg: number;
  blk_allowed_avg: number;
  pa_allowed_avg: number;
  pr_allowed_avg: number;
  pra_allowed_avg: number;
  fg3m_allowed_avg: number;
  dd_rate_allowed: number;
  td_rate_allowed: number;
  pts_allowed_last10_list?: number[];
  reb_allowed_last10_list?: number[];
  ast_allowed_last10_list?: number[];
  fg3m_allowed_last10_list?: number[];
  pts_allowed_rank: number;
  reb_allowed_rank: number;
  ast_allowed_rank: number;
  stl_allowed_rank: number;
  blk_allowed_rank: number;
  pa_allowed_rank: number;
  pr_allowed_rank: number;
  pra_allowed_rank: number;
  fg3m_allowed_rank: number;
  dd_rate_allowed_rank: number;
  td_rate_allowed_rank: number;
  computed_at: string;
};

export function useOpponentPositionDefense() {
  const [data, setData] = useState<OpponentPositionDefenseRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const fetchData = useCallback(async () => {
    try {
      setLoading(true);
      setError(null);
      const response = await fetch(`${API}/opponent-position-defense`);
      if (!response.ok) {
        throw new Error("Failed to load opponent position defense");
      }
      const rows: OpponentPositionDefenseRow[] = await response.json();
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

// hooks/useHitRateMatrix.ts
import { useState, useEffect, useCallback } from "react";
import { API_BASE } from "@/lib/config";

export type HitRateCell = {
  hit: number;
  total: number;
};

export type HitRatePlayer = {
  player_id: string;
  player_name: string;
  position: string;
  team_code: string;
  opp_team_code: string;
  line: number;
  matchup_rank: number | null;
  matchup_label: string | null;
  avg_l10: number | null;
  dk_price: number | null;
  dk_deep_link: string | null;
  dk_event_id: string | null;
  dk_outcome_code: string | null;
  fd_price: number | null;
  fd_deep_link: string | null;
  fd_market_id: string | null;
  fd_selection_id: string | null;
  best_book: string | null;
  best_price: number | null;
  game_id: string;
  szn_avg: number | null;
  cells: Record<string, HitRateCell>;
  game_values: number[];
};

export type GameOption = {
  game_id: string;
  label: string;
  away_team_code?: string;
  home_team_code?: string;
  date?: string;
  time?: string;
  spread?: string;
  total?: string;
};

export type HitRateMatrixData = {
  thresholds: number[];
  players: HitRatePlayer[];
  games: GameOption[];
  category: string;
  game_count: string;
};

type UseHitRateMatrixArgs = {
  category: string;
  position: string;
  gameCount: string;
  gameIds?: string[];
};

export function useHitRateMatrix({
  category,
  position,
  gameCount,
  gameIds,
}: UseHitRateMatrixArgs) {
  const [data, setData] = useState<HitRateMatrixData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const fetchMatrix = useCallback(async () => {
    setLoading(true);
    setError(null);

    const params = new URLSearchParams();
    params.set("category", category);
    params.set("position", position);
    params.set("game_count", gameCount);
    if (gameIds && gameIds.length > 0) {
      params.set("game_ids", gameIds.join(","));
    }

    const url = `${API_BASE}hit-rate-matrix?${params.toString()}`;

    try {
      const res = await fetch(url, { credentials: "omit" });
      if (!res.ok) {
        throw new Error(`HTTP ${res.status}`);
      }
      const json: HitRateMatrixData = await res.json();
      setData(json);
    } catch (e: any) {
      setError(e.message || "Failed to fetch hit rate matrix");
      setData(null);
    } finally {
      setLoading(false);
    }
  }, [category, position, gameCount, gameIds?.join(",")]);

  useEffect(() => {
    fetchMatrix();
  }, [fetchMatrix]);

  return { data, loading, error, refetch: fetchMatrix };
}

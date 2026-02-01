// hooks/useWowy.ts
import { useCallback, useEffect, useState } from "react";
import {
  fetchWowyForInjured,
  fetchWowyForPlayer,
  fetchBeneficiaries,
  fetchCachedWowy, // ✅ ADD THIS
  WowyResponse,
  SinglePlayerWowyResponse,
  BeneficiariesResponse,
  InjuredPlayerWowy,
} from "@/lib/wowy";


type UseWowyOptions = {
  team?: string;
  status?: string;
  season?: number;
};

/**
 * Hook to get WOWY analysis for all currently injured players
 */
export function useWowy(options: UseWowyOptions = {}) {
  const [data, setData] = useState<WowyResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedStat, setSelectedStat] = useState<
    "pts" | "reb" | "ast" | "fg3m"
  >("pts");

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await fetchCachedWowy({
        season: options.season ?? 2025,
        stat: selectedStat, // pts | reb | ast | fg3m
      });

      setData(response);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unknown error");
    } finally {
      setLoading(false);
    }
  }, [options.team, options.status, options.season, selectedStat]);


  useEffect(() => {
    load();
  }, [load]);

  return {
    data,
    injuredPlayers: data?.injured_players ?? [],
    count: data?.count ?? 0,
    season: data?.season ?? null,
    loading,
    error,
    refresh: load,
  };
}

/**
 * Hook to get WOWY analysis for a specific player
 */
export function usePlayerWowy(
  playerId: number | null,
  options?: {
    season?: number;
    minGamesWith?: number;
    minGamesWithout?: number;
  }
) {
  const [data, setData] = useState<SinglePlayerWowyResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    if (!playerId) {
      setData(null);
      return;
    }

    setLoading(true);
    setError(null);
    try {
      const response = await fetchWowyForPlayer(playerId, options);
      setData(response);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unknown error");
    } finally {
      setLoading(false);
    }
  }, [playerId, options?.season, options?.minGamesWith, options?.minGamesWithout]);

  useEffect(() => {
    load();
  }, [load]);

  return {
    data,
    targetPlayer: data?.target_player ?? null,
    teamImpact: data?.team_impact ?? null,
    teammates: data?.teammates ?? [],
    loading,
    error,
    refresh: load,
  };
}

/**
 * Hook to get top beneficiaries when a player is out
 */
export function useBeneficiaries(
  playerId: number | null,
  options?: {
    stat?: "pts" | "reb" | "ast" | "fg3m" | "min";
    limit?: number;
    season?: number;
  }
) {
  const [data, setData] = useState<BeneficiariesResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    if (!playerId) {
      setData(null);
      return;
    }

    setLoading(true);
    setError(null);
    try {
      const response = await fetchBeneficiaries(playerId, options);
      setData(response);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unknown error");
    } finally {
      setLoading(false);
    }
  }, [playerId, options?.stat, options?.limit, options?.season]);

  useEffect(() => {
    load();
  }, [load]);

  return {
    beneficiaries: data?.beneficiaries ?? [],
    stat: data?.stat ?? null,
    loading,
    error,
    refresh: load,
  };
}

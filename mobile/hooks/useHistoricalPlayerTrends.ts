import { useEffect, useRef, useState } from "react";
import Constants from "expo-constants";

/* ======================================================
   CONFIG
====================================================== */
const API = Constants.expoConfig?.extra?.API_URL!;

/* ======================================================
   TYPES (Option A — canonical market_key support)
====================================================== */
export type HistoricalPlayerTrend = {
  player: string;

  /* ======================
     CORE STATS
  ====================== */
  pts_last5_list?: number[];
  pts_last10_list?: number[];
  pts_last20_list?: number[];

  reb_last5_list?: number[];
  reb_last10_list?: number[];
  reb_last20_list?: number[];

  ast_last5_list?: number[];
  ast_last10_list?: number[];
  ast_last20_list?: number[];

  stl_last5_list?: number[];
  stl_last10_list?: number[];
  stl_last20_list?: number[];

  blk_last5_list?: number[];
  blk_last10_list?: number[];
  blk_last20_list?: number[];

  fg3m_last5_list?: number[];
  fg3m_last10_list?: number[];
  fg3m_last20_list?: number[];

  /* ======================
     COMBO MARKETS
  ====================== */
  pr_last5_list?: number[];
  pr_last10_list?: number[];
  pr_last20_list?: number[];

  pa_last5_list?: number[];
  pa_last10_list?: number[];
  pa_last20_list?: number[];

  ra_last5_list?: number[];
  ra_last10_list?: number[];
  ra_last20_list?: number[];

  pra_last5_list?: number[];
  pra_last10_list?: number[];
  pra_last20_list?: number[];

  /* ======================
     MILESTONES
  ====================== */
  dd_last5_list?: number[];
  dd_last10_list?: number[];
  dd_last20_list?: number[];

  td_last5_list?: number[];
  td_last10_list?: number[];
  td_last20_list?: number[];

  /* ======================
     DATES
  ====================== */
  last5_dates?: string[];
  last10_dates?: string[];
  last20_dates?: string[];
};

/* ======================================================
   HOOK
====================================================== */
export function useHistoricalPlayerTrends() {
  const cacheRef = useRef<Record<string, HistoricalPlayerTrend>>({});
  const [ready, setReady] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;

    async function load() {
      try {
        const res = await fetch(`${API}/historical/player-trends`);
        if (!res.ok) throw new Error("Failed to fetch player trends");

        const rows: HistoricalPlayerTrend[] = await res.json();
        if (cancelled) return;

        const map: Record<string, HistoricalPlayerTrend> = {};

        for (const row of rows) {
          if (!row.player) continue;
          map[row.player] = row;
        }

        cacheRef.current = map;
        setReady(true);
      } catch (e: any) {
        if (!cancelled) {
          console.warn("❌ historical trends load failed", e);
          setError(e.message ?? "unknown error");
        }
      }
    }

    load();
    return () => {
      cancelled = true;
    };
  }, []);

  /* ===========================
     SELECTORS
  =========================== */

  const getByPlayer = (player: string) =>
    cacheRef.current[player];

  const players = Object.keys(cacheRef.current).sort();

  return {
    ready,
    error,
    getByPlayer,
    players,
  };
}

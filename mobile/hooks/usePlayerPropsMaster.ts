// hooks/usePlayerPropsMaster.ts
import { useEffect, useMemo, useState } from "react";
import { fetchPlayerPropsMaster } from "@/lib/apiMaster";

/* ======================================================
   Types
====================================================== */
export type HitRateWindow = "L5" | "L10" | "L20";
export type BetSide = "OVER" | "UNDER";

type Filters = {
  market: string;
  hitRateWindow: HitRateWindow;
  minHitRate: number;
  minOdds: number;
  maxOdds: number;
  side: BetSide;
};

/* ======================================================
   Defaults
====================================================== */
const DEFAULT_FILTERS: Filters = {
  market: "ALL",
  hitRateWindow: "L10",
  minHitRate: 80,
  minOdds: -700,
  maxOdds: 200,
  side: "OVER",
};

/* ======================================================
   Hook
====================================================== */
export function usePlayerPropsMaster() {
  const [raw, setRaw] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [filters, setFilters] = useState<Filters>(DEFAULT_FILTERS);

  /* ===============================
     Fetch
  ================================ */
  useEffect(() => {
    fetchPlayerPropsMaster()
      .then((rows) => setRaw(rows ?? []))
      .finally(() => setLoading(false));
  }, []);

  /* ===============================
     Markets
  ================================ */
  const markets = useMemo(() => {
    const uniq = new Set<string>();
    raw.forEach((r) => {
      if (r?.prop_type_base) uniq.add(r.prop_type_base);
    });
    return ["ALL", ...Array.from(uniq)];
  }, [raw]);

  /* ===============================
     Filter + Normalize
  ================================ */
  const props = useMemo(() => {
    return raw
      .filter((p) => {
        // ---------- Hard guards ----------
        if (!p) return false;
        if (!p.player_id) return false;
        if (!p.prop_type_base) return false;
        if (p.line_value == null) return false;

        // ---------- Market ----------
        if (filters.market !== "ALL" && p.prop_type_base !== filters.market) {
          return false;
        }

        // ---------- Hit rate ----------
        const hit =
          filters.side === "OVER"
            ? filters.hitRateWindow === "L5"
              ? p.hit_rate_over_l5
              : filters.hitRateWindow === "L20"
              ? p.hit_rate_over_l20
              : p.hit_rate_over_l10
            : filters.hitRateWindow === "L5"
            ? p.hit_rate_under_l5
            : filters.hitRateWindow === "L20"
            ? p.hit_rate_under_l20
            : p.hit_rate_under_l10;

        if ((hit ?? 0) * 100 < filters.minHitRate) return false;

        // ---------- Odds ----------
        const odds =
          filters.side === "OVER" ? p.odds_over : p.odds_under;

        if (odds == null) return false;
        if (odds < filters.minOdds || odds > filters.maxOdds) return false;

        return true;
      })
      .map((p, idx) => {
        // ---------- Lists ----------
        const list =
          filters.hitRateWindow === "L5"
            ? p.list_l5
            : filters.hitRateWindow === "L20"
            ? p.list_l20
            : p.list_l10;

        const odds =
          filters.side === "OVER" ? p.odds_over : p.odds_under;

        return {
          ...p,

          // stable, collision-proof ID
          id: [
            p.player_id,
            p.prop_type_base,
            p.line_value,
            p.market_window ?? "FULL",
            filters.side,
            idx,
          ].join("::"),

          // normalized fields used by UI
          player: p.player_name,
          market: p.prop_type_base,
          line: p.line_value,
          odds,
          list,
          hit_rate: filters.side === "OVER"
            ? filters.hitRateWindow === "L5"
              ? p.hit_rate_over_l5
              : filters.hitRateWindow === "L20"
              ? p.hit_rate_over_l20
              : p.hit_rate_over_l10
            : filters.hitRateWindow === "L5"
            ? p.hit_rate_under_l5
            : filters.hitRateWindow === "L20"
            ? p.hit_rate_under_l20
            : p.hit_rate_under_l10,
        };
      });
  }, [raw, filters]);

  /* ===============================
     Public API
  ================================ */
  return {
    loading,
    props,
    filters: {
      ...filters,
      markets,
    },
    setFilters,
  };
}
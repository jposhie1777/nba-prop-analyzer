// hooks/usePlayerPropsMaster.ts
import { useEffect, useMemo, useState, useCallback } from "react";
import { fetchPlayerPropsMaster } from "@/lib/apiMaster";

export type HitRateWindow = "L5" | "L10" | "L20";

type Filters = {
  market: string;
  hitRateWindow: HitRateWindow;
  minHitRate: number;
  minOdds: number;
  maxOdds: number;
};

type FetchArgs = {
  limit?: number;
  offset?: number;
};

/* ======================================================
   DEFAULT FILTERS
====================================================== */
const DEFAULT_FILTERS: Filters = {
  market: "ALL",
  hitRateWindow: "L5",
  minHitRate: 0,
  minOdds: -1000,
  maxOdds: 1000,
};

/* ======================================================
   HOOK
====================================================== */
export function usePlayerPropsMaster({
  limit = 200,
}: {
  limit?: number;
} = {}) {
  const [raw, setRaw] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [filters, setFilters] = useState<Filters>(DEFAULT_FILTERS);

  /* ======================================================
     INITIAL FETCH (PAGE 0)
  ====================================================== */
  useEffect(() => {
    let mounted = true;

    setLoading(true);

    fetchPlayerPropsMaster({ limit, offset: 0 })
      .then((rows) => {
        if (!mounted) return;
        console.log("📦 [MASTER] initial rows:", rows.length);
        setRaw(rows);
      })
      .catch((e) => {
        console.error("❌ [MASTER] fetch failed", e);
      })
      .finally(() => mounted && setLoading(false));

    return () => {
      mounted = false;
    };
  }, [limit]);

  /* ======================================================
     FETCH NEXT PAGE
  ====================================================== */
  const fetchNext = useCallback(
    async ({ offset }: FetchArgs = {}) => {
      const rows = await fetchPlayerPropsMaster({
        limit,
        offset,
      });

      console.log("📦 [MASTER] fetched next:", rows.length);

      if (rows.length) {
        setRaw((prev) => [...prev, ...rows]);
      }

      return rows.length;
    },
    [limit]
  );

  /* ======================================================
     MARKET LIST (FROM LOADED DATA)
  ====================================================== */
  const markets = useMemo(() => {
    const set = new Set<string>();
    raw.forEach((r) => r.prop_type_base && set.add(r.prop_type_base));
    return ["ALL", ...Array.from(set)];
  }, [raw]);

  /* ======================================================
     FILTER + NORMALIZE
  ====================================================== */
  const props = useMemo(() => {
    const filtered = raw.filter((p) => {
      /* ---------- MARKET ---------- */
      if (
        filters.market !== "ALL" &&
        p.prop_type_base !== filters.market
      ) {
        return false;
      }

      /* ---------- ODDS ---------- */
      if (
        p.odds == null ||
        p.odds < filters.minOdds ||
        p.odds > filters.maxOdds
      ) {
        return false;
      }

      /* ---------- HIT RATE ---------- */
      const hitRate =
        filters.hitRateWindow === "L5"
          ? p.hit_rate_l5
          : filters.hitRateWindow === "L20"
          ? p.hit_rate_l20
          : p.hit_rate_l10;

      if (hitRate == null || hitRate * 100 < filters.minHitRate) {
        return false;
      }

      return true;
    });

    return filtered.map((p, idx) => ({
      /* ---------- KEYS ---------- */
      id: `${p.prop_id}-${p.odds_side}-${idx}`,
      propId: p.prop_id,

      /* ---------- DISPLAY ---------- */
      player: p.player_name,
      market: p.prop_type_base,
      window: p.market_window,
      line: p.line_value,

      /* ---------- ODDS ---------- */
      odds: p.odds,
      oddsSide: p.odds_side,
      bookmaker: p.bookmaker_key,

      /* ---------- HIT RATES ---------- */
      hit_rate_l5: p.hit_rate_l5,
      hit_rate_l10: p.hit_rate_l10,
      hit_rate_l20: p.hit_rate_l20,

      hitRate:
        filters.hitRateWindow === "L5"
          ? p.hit_rate_l5
          : filters.hitRateWindow === "L20"
          ? p.hit_rate_l20
          : p.hit_rate_l10,

      hitRatePct: Math.round(
        ((filters.hitRateWindow === "L5"
          ? p.hit_rate_l5
          : filters.hitRateWindow === "L20"
          ? p.hit_rate_l20
          : p.hit_rate_l10) ?? 0) * 100
      ),
    }));
  }, [raw, filters]);

  /* ======================================================
     RETURN
  ====================================================== */
  return {
    loading,
    props,
    filters: { ...filters, markets },
    setFilters,
    fetchNext, // ✅ REQUIRED BY SCREEN
  };
}}
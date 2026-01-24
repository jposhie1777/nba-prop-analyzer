// hooks/usePlayerPropsMaster.ts
import { useEffect, useMemo, useState } from "react";
import { fetchPlayerPropsMaster } from "@/lib/apiMaster";

export type HitRateWindow = "L5" | "L10" | "L20";

type Filters = {
  market: string;
  hitRateWindow: HitRateWindow;
  minHitRate: number;
  minOdds: number;
  maxOdds: number;
};

/* ======================================================
   DEFAULT FILTERS (DISCOVERY-FIRST)
====================================================== */
const DEFAULT_FILTERS: Filters = {
  market: "ALL",
  hitRateWindow: "L5",
  minHitRate: 0,
  minOdds: -1000,
  maxOdds: 1000,
};

export function usePlayerPropsMaster() {
  const [raw, setRaw] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [filters, setFilters] = useState<Filters>(DEFAULT_FILTERS);

  /* ======================================================
     FETCH
  ====================================================== */
  useEffect(() => {
    let mounted = true;

    fetchPlayerPropsMaster()
      .then((rows) => {
        if (!mounted) return;
        console.log("📦 [MASTER] raw rows:", rows.length);
        console.log("📦 [MASTER] sample raw:", rows[0]);
        setRaw(rows);
      })
      .catch((e) => {
        console.error("❌ [MASTER] fetch failed", e);
      })
      .finally(() => mounted && setLoading(false));

    return () => {
      mounted = false;
    };
  }, []);

  /* ======================================================
     MARKET LIST (FROM DATA)
  ====================================================== */
  const markets = useMemo(() => {
    const set = new Set<string>();
    raw.forEach((r) => r.prop_type_base && set.add(r.prop_type_base));
    const result = ["ALL", ...Array.from(set)];
    console.log("📊 [MASTER] markets:", result);
    return result;
  }, [raw]);

  /* ======================================================
     FILTER + NORMALIZE
  ====================================================== */
  const props = useMemo(() => {
    let failMarket = 0;
    let failOdds = 0;
    let failHit = 0;
    let pass = 0;

    const filtered = raw.filter((p) => {
      /* ---------- MARKET ---------- */
      if (
        filters.market !== "ALL" &&
        p.prop_type_base !== filters.market
      ) {
        failMarket++;
        return false;
      }

      /* ---------- ODDS ---------- */
      if (
        p.odds == null ||
        p.odds < filters.minOdds ||
        p.odds > filters.maxOdds
      ) {
        failOdds++;
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
        failHit++;
        return false;
      }

      pass++;
      return true;
    });

    console.log("✅ [MASTER] filter results:", {
      failMarket,
      failOdds,
      failHit,
      pass,
    });

    const normalized = filtered.map((p, idx) => ({
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

      /* ---------- HIT RATES (ALL WINDOWS) ---------- */
      hit_rate_l5: p.hit_rate_l5,
      hit_rate_l10: p.hit_rate_l10,
      hit_rate_l20: p.hit_rate_l20,

      /* ---------- ACTIVE WINDOW ---------- */
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

    console.log("🧪 [MASTER] sample final:", normalized[0]);
    return normalized;
  }, [raw, filters]);

  /* ======================================================
     RETURN
  ====================================================== */
  return {
    loading,
    props,
    filters: { ...filters, markets },
    setFilters,
  };
}
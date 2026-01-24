// hooks/usePlayerPropsMaster.ts
import { useEffect, useMemo, useState } from "react";
import { fetchPlayerPropsMaster } from "@/lib/apiMaster";

export type HitRateWindow = "L5" | "L10" | "L20";

const DEFAULT_FILTERS = {
  market: "ALL",
  hitRateWindow: "L10" as HitRateWindow,
  minHitRate: 60,      // 🔑 lowered default
  minOdds: -700,
  maxOdds: 200,
};

export function usePlayerPropsMaster() {
  const [raw, setRaw] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [filters, setFilters] = useState(DEFAULT_FILTERS);

  /* ============================
     Fetch
  ============================ */
  useEffect(() => {
    fetchPlayerPropsMaster()
      .then(setRaw)
      .finally(() => setLoading(false));
  }, []);

  /* ============================
     Markets
  ============================ */
  const markets = useMemo(() => {
    const set = new Set<string>();
    raw.forEach((r) => r.prop_type_base && set.add(r.prop_type_base));
    return ["ALL", ...Array.from(set)];
  }, [raw]);

  /* ============================
     Filtered + normalized props
  ============================ */
  const props = useMemo(() => {
    return raw
      .filter((p) => {
        /* MARKET */
        if (
          filters.market !== "ALL" &&
          p.prop_type_base !== filters.market
        ) {
          return false;
        }

        /* ODDS */
        if (p.odds == null) return false;
        if (p.odds < filters.minOdds || p.odds > filters.maxOdds) {
          return false;
        }

        /* HIT RATE — side aware */
        const hit =
          p.odds_side === "UNDER"
            ? filters.hitRateWindow === "L5"
              ? p.hit_rate_under_l5
              : filters.hitRateWindow === "L20"
              ? p.hit_rate_under_l20
              : p.hit_rate_under_l10
            : filters.hitRateWindow === "L5"
            ? p.hit_rate_over_l5
            : filters.hitRateWindow === "L20"
            ? p.hit_rate_over_l20
            : p.hit_rate_over_l10;

        if (hit == null) return false;
        if (hit * 100 < filters.minHitRate) return false;

        return true;
      })
      .map((p) => ({
        /* ========= identity ========= */
        id: `${p.prop_id}-${p.odds_side}`, // 🔒 guaranteed unique
        propId: p.prop_id,

        /* ========= display ========= */
        player: p.player_name,
        market: p.prop_type_base,
        window: p.market_window,
        line: p.line_value,

        odds: p.odds,
        oddsSide: p.odds_side,

        /* ========= analytics ========= */
        hitRate:
          p.odds_side === "UNDER"
            ? filters.hitRateWindow === "L5"
              ? p.hit_rate_under_l5
              : filters.hitRateWindow === "L20"
              ? p.hit_rate_under_l20
              : p.hit_rate_under_l10
            : filters.hitRateWindow === "L5"
            ? p.hit_rate_over_l5
            : filters.hitRateWindow === "L20"
            ? p.hit_rate_over_l20
            : p.hit_rate_over_l10,
      }));
  }, [raw, filters]);

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
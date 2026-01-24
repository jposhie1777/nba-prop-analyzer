// hooks/usePlayerPropsMaster.ts
import { useEffect, useMemo, useState } from "react";
import { fetchPlayerPropsMaster } from "@/lib/apiMaster";

export type HitRateWindow = "L5" | "L10" | "L20";

const DEFAULT_FILTERS = {
  market: "ALL",
  hitRateWindow: "L10" as HitRateWindow,
  minHitRate: 60,
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
      .then((rows) => {
        if (__DEV__) {
          console.log("📦 [MASTER] raw rows:", rows.length);
          console.log("📦 [MASTER] sample row:", rows[0]);
        }
        setRaw(rows);
      })
      .finally(() => setLoading(false));
  }, []);

  /* ============================
     Markets
  ============================ */
  const markets = useMemo(() => {
    const set = new Set<string>();
    raw.forEach((r) => r.prop_type_base && set.add(r.prop_type_base));

    const result = ["ALL", ...Array.from(set)];

    if (__DEV__) {
      console.log("🏷️ [MASTER] markets:", result);
    }

    return result;
  }, [raw]);

  /* ============================
     Filtered + normalized props
  ============================ */
  const props = useMemo(() => {
    let pass = 0;
    let failMarket = 0;
    let failOdds = 0;
    let failHit = 0;

    if (__DEV__) {
      console.log("🔍 [MASTER] filter input:", {
        raw: raw.length,
        filters,
      });
    }

    const result = raw
      .filter((p) => {
        /* MARKET */
        if (
          filters.market !== "ALL" &&
          p.prop_type_base !== filters.market
        ) {
          failMarket++;
          return false;
        }

        /* ODDS */
        if (p.odds == null) {
          failOdds++;
          return false;
        }
        if (p.odds < filters.minOdds || p.odds > filters.maxOdds) {
          failOdds++;
          return false;
        }

        /* HIT RATE */
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

        if (hit == null || hit * 100 < filters.minHitRate) {
          failHit++;
          return false;
        }

        pass++;
        return true;
      })
      .map((p) => ({
        id: `${p.prop_id}-${p.odds_side}`,
        propId: p.prop_id,

        player: p.player_name,
        market: p.prop_type_base,
        window: p.market_window,
        line: p.line_value,

        odds: p.odds,
        oddsSide: p.odds_side,

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

    if (__DEV__) {
      console.log("✅ [MASTER] filter results:", {
        pass,
        failMarket,
        failOdds,
        failHit,
      });

      if (result.length > 0) {
        console.log("🧪 [MASTER] sample final prop:", result[0]);
      }
    }

    return result;
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
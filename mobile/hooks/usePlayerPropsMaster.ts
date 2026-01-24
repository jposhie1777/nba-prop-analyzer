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
     FETCH
  ============================ */
  useEffect(() => {
    fetchPlayerPropsMaster()
      .then((rows) => {
        console.log("📦 [MASTER] raw rows:", rows.length);
        console.log("📦 [MASTER] sample row:", rows[0]);
        setRaw(rows);
      })
      .catch((e) => {
        console.error("❌ [MASTER] fetch failed", e);
      })
      .finally(() => setLoading(false));
  }, []);

  /* ============================
     MARKET LIST
  ============================ */
  const markets = useMemo(() => {
    const set = new Set<string>();
    raw.forEach((r) => r.prop_type_base && set.add(r.prop_type_base));
    const result = ["ALL", ...Array.from(set)];

    console.log("📊 [MASTER] markets:", result);
    return result;
  }, [raw]);

  /* ============================
     FILTER + NORMALIZE
  ============================ */
  const props = useMemo(() => {
    let failMarket = 0;
    let failOdds = 0;
    let failHit = 0;
    let pass = 0;

    const out = raw.filter((p) => {
      /* MARKET */
      if (filters.market !== "ALL" && p.prop_type_base !== filters.market) {
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

      /* HIT RATE (side-aware) */
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

      if (hit == null) {
        failHit++;
        return false;
      }

      if (hit * 100 < filters.minHitRate) {
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

    const normalized = out.map((p) => ({
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

    console.log("🧪 [MASTER] sample final prop:", normalized[0]);
    return normalized;
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
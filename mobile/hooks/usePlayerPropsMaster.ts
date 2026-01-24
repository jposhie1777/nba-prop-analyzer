// hooks/usePlayerPropsMaster.ts
import { useEffect, useMemo, useState } from "react";
import { fetchPlayerPropsMaster } from "@/lib/apiMaster";

type HitRateWindow = "L5" | "L10" | "L20";

const DEFAULT_FILTERS = {
  market: "ALL",
  hitRateWindow: "L10" as HitRateWindow,
  minHitRate: 80,
  minOdds: -700,
  maxOdds: 200,
};

export function usePlayerPropsMaster() {
  const [raw, setRaw] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [filters, setFilters] = useState(DEFAULT_FILTERS);

  useEffect(() => {
    fetchPlayerPropsMaster()
      .then((rows) => setRaw(rows))
      .finally(() => setLoading(false));
  }, []);

  const markets = useMemo(() => {
    return ["ALL", ...Array.from(new Set(raw.map(r => r.prop_type_base)))];
  }, [raw]);

  const props = useMemo(() => {
    return raw
      .filter((p) => {
        // MARKET
        if (filters.market !== "ALL" && p.prop_type_base !== filters.market) {
          return false;
        }

        // HIT RATE
        const hit =
          filters.hitRateWindow === "L5"
            ? p.hit_rate_over_l5
            : filters.hitRateWindow === "L20"
            ? p.hit_rate_over_l20
            : p.hit_rate_over_l10;

        if ((hit ?? 0) * 100 < filters.minHitRate) return false;

        // ODDS (prefer over)
        const odds = p.odds_over ?? p.odds_under;
        if (odds == null) return false;
        if (odds < filters.minOdds || odds > filters.maxOdds) return false;

        return true;
      })
      .map((p) => ({
        ...p,
        id: `${p.player_id}-${p.prop_type_base}-${p.line_value}-${p.market_window}`,
        player: p.player_name,
        market: p.prop_type_base,
        line: p.line_value,
        odds: p.odds_over ?? p.odds_under,
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

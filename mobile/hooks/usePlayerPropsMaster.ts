import { useEffect, useMemo, useState } from "react";
import { fetchPlayerPropsMaster } from "@/lib/apiMaster";

const DEFAULT_FILTERS = {
  market: "ALL",
  hitRateWindow: "L10" as "L5" | "L10" | "L20",
  minHitRate: 80,
  minConfidence: 0,
  minOdds: -700,
  maxOdds: 200,
};

export function usePlayerPropsMaster() {
  const [raw, setRaw] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [filters, setFilters] = useState(DEFAULT_FILTERS);

  useEffect(() => {
    fetchPlayerPropsMaster().then((rows) => {
      setRaw(rows);
      setLoading(false);
    });
  }, []);

  const markets = useMemo(() => {
    return ["ALL", ...Array.from(new Set(raw.map(r => r.prop_type_base)))];
  }, [raw]);

  const props = useMemo(() => {
    return raw
      .filter((p) => {
        if (filters.market !== "ALL" && p.prop_type_base !== filters.market)
          return false;

        const hit =
          filters.hitRateWindow === "L5"
            ? p.hit_rate_l5
            : filters.hitRateWindow === "L20"
            ? p.hit_rate_l20
            : p.hit_rate_l10;

        if ((hit ?? 0) * 100 < filters.minHitRate) return false;
        if (p.confidence < filters.minConfidence) return false;
        if (p.odds < filters.minOdds || p.odds > filters.maxOdds) return false;

        return true;
      })
      .map((p) => ({
        ...p,
        id: `${p.player}-${p.prop_type_base}-${p.line_value}`,
        market: p.prop_type_base,
        line: p.line_value,
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

// hooks/usePlayerPropsMaster.ts
import { useEffect, useMemo, useState, useCallback } from "react";
import { fetchPlayerPropsMaster } from "@/lib/apiMaster";

export type HitRateWindow = "L5" | "L10" | "L20";

type Filters = {
  market: string;
  marketWindow: "FULL" | "Q1" | "FIRST3MIN" | null;
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
  marketWindow: null,   // ✅
  hitRateWindow: "L5",
  minHitRate: 0,
  minOdds: -750,
  maxOdds: 500,
};


/* ======================================================
   HOOK
====================================================== */
export function usePlayerPropsMaster({
  limit = 800,
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

      // 🔴 ADD THIS LINE
      console.log("🧪 [MASTER] sample row keys:", Object.keys(rows[0] ?? {}));
      console.log("🧪 [MASTER] sample row:", rows[0]);

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
    DEBUG — MARKET KEYS (PART 5)
  ====================================================== */
  useEffect(() => {
    if (!raw.length) return;

    console.log(
      "🧪 MARKET KEYS:",
      Array.from(new Set(raw.map(r => r.market_key)))
    );
  }, [raw]);


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
  const MARKET_ORDER = [
    "pts",
    "reb",
    "ast",
    "3pm",
    "stl",
    "blk",
    "tov",
    "pr",
    "pa",
    "ra",
    "pra",
    "dd",
    "td",
  ];

  const markets = useMemo(() => {
    const set = new Set<string>();
    raw.forEach((r) => {
      if (r.market_key) set.add(r.market_key);
    });

    return [
      "ALL",
      ...MARKET_ORDER.filter((m) => set.has(m)),
      ...Array.from(set).filter((m) => !MARKET_ORDER.includes(m)),
    ];
  }, [raw]);


  /* ======================================================
     FILTER + NORMALIZE
  ====================================================== */
  const props = useMemo(() => {
    const filtered = raw.filter((p) => {

      /* ---------- MARKET ---------- */
      if (
        filters.market !== "ALL" &&
        p.market_key !== filters.market
      ) {
        return false;
      }


      /* ---------- MARKET WINDOW ---------- */
      if (
        filters.marketWindow &&
        p.market_window !== filters.marketWindow
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

    return filtered.map((p, idx) => {
      const hitRate =
        filters.hitRateWindow === "L5"
          ? p.hit_rate_l5
          : filters.hitRateWindow === "L20"
          ? p.hit_rate_l20
          : p.hit_rate_l10;

      return {
        /* ---------- KEYS ---------- */
        id: `${p.prop_id}-${p.odds_side}-${idx}`,
        propId: p.prop_id,

        /* ---------- DISPLAY ---------- */
        player_id: p.player_id,
        player: p.player_name,
        market: p.market_key,
        window: p.market_window,
        line: p.line_value,

        /* ---------- ODDS ---------- */
        odds: p.odds,
        side: p.odds_side,
        bookmaker: p.vendor,

        /* ---------- MEDIA ---------- */
        playerImageUrl: p.player_image_url,
        homeTeam: p.home_team_abbr,
        awayTeam: p.away_team_abbr,

        /* ---------- HIT RATES ---------- */
        hit_rate_l5: p.hit_rate_l5,
        hit_rate_l10: p.hit_rate_l10,
        hit_rate_l20: p.hit_rate_l20,

        hitRate,
        hitRatePct: Math.round((hitRate ?? 0) * 100),
      };
    });
  }, [raw, filters]);



  /* ======================================================
     RETURN
  ====================================================== */
    return {
    loading,
    props,
    filters: { ...filters, markets },
    setFilters,
    fetchNext,
  };
}
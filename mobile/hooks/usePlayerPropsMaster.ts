// hooks/usePlayerPropsMaster.ts
import { useEffect, useMemo, useState, useCallback } from "react";
import { fetchPlayerPropsMaster } from "@/lib/apiMaster";
import { TEAM_LOGOS } from "@/utils/teamLogos";

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

const TEAM_ABBRS = new Set(Object.keys(TEAM_LOGOS));

const TEAM_NAME_TO_ABBR: Record<string, string> = {
  "atlanta hawks": "ATL",
  "boston celtics": "BOS",
  "brooklyn nets": "BKN",
  "charlotte hornets": "CHA",
  "chicago bulls": "CHI",
  "cleveland cavaliers": "CLE",
  "dallas mavericks": "DAL",
  "denver nuggets": "DEN",
  "detroit pistons": "DET",
  "golden state warriors": "GSW",
  "houston rockets": "HOU",
  "indiana pacers": "IND",
  "la clippers": "LAC",
  "los angeles clippers": "LAC",
  "la lakers": "LAL",
  "los angeles lakers": "LAL",
  "memphis grizzlies": "MEM",
  "miami heat": "MIA",
  "milwaukee bucks": "MIL",
  "minnesota timberwolves": "MIN",
  "new orleans pelicans": "NOP",
  "new york knicks": "NYK",
  "oklahoma city thunder": "OKC",
  "orlando magic": "ORL",
  "philadelphia 76ers": "PHI",
  "philadelphia sixers": "PHI",
  "phoenix suns": "PHX",
  "portland trail blazers": "POR",
  "sacramento kings": "SAC",
  "san antonio spurs": "SAS",
  "toronto raptors": "TOR",
  "utah jazz": "UTA",
  "washington wizards": "WAS",
};

function normalizeTeamAbbr(value: unknown): string | undefined {
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim();
  if (!trimmed) return undefined;

  const upper = trimmed.toUpperCase();
  if (TEAM_ABBRS.has(upper)) return upper;

  const normalizedName = trimmed
    .toLowerCase()
    .replace(/\./g, "")
    .replace(/\s+/g, " ");

  return TEAM_NAME_TO_ABBR[normalizedName];
}

function parseMatchup(matchup: unknown): {
  home?: string;
  away?: string;
} {
  if (typeof matchup !== "string") return {};
  const normalized = matchup.trim();
  if (!normalized) return {};

  if (normalized.includes("@")) {
    const [awayRaw, homeRaw] = normalized.split("@");
    return {
      away: normalizeTeamAbbr(awayRaw),
      home: normalizeTeamAbbr(homeRaw),
    };
  }

  const vsParts = normalized.split(/\s+vs\.?\s+/i);
  if (vsParts.length === 2) {
    return {
      home: normalizeTeamAbbr(vsParts[0]),
      away: normalizeTeamAbbr(vsParts[1]),
    };
  }

  return {};
}

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
  limit = 600,
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

      const matchupTeams = parseMatchup(
        p.matchup ?? p.game_matchup ?? p.matchup_display
      );

      // 🔍 DEBUG — confirm whether teams exist at the DATA level
      if (__DEV__ && (!p.home_team_abbr || !p.away_team_abbr)) {
        console.warn("🚨 PROP HAS NO TEAMS", {
          prop_id: p.prop_id,
          game_id: p.game_id,
          player: p.player_name,
          home_team_abbr: p.home_team_abbr,
          away_team_abbr: p.away_team_abbr,
          matchup: p.matchup ?? p.game_matchup ?? p.matchup_display,
        });
      }

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
        homeTeam:
          normalizeTeamAbbr(
            p.home_team_abbr ??
              p.home_team_abbrev ??
              p.homeTeam ??
              p.home_team ??
              p.home_team?.abbreviation ??
              p.home_abbr ??
              p.home
          ) ?? matchupTeams.home,
        awayTeam:
          normalizeTeamAbbr(
            p.away_team_abbr ??
              p.away_team_abbrev ??
              p.visitor_team_abbr ??
              p.awayTeam ??
              p.away_team ??
              p.away_team?.abbreviation ??
              p.away_abbr ??
              p.away
          ) ?? matchupTeams.away,
        playerTeamAbbr:
          normalizeTeamAbbr(
            p.player_team_abbr ??
              p.team_abbr ??
              p.player_team ??
              p.team ??
              p.team_abbreviation ??
              p.team_abbrev
          ) ?? undefined,
        opponentTeamAbbr:
          normalizeTeamAbbr(
            p.opponent_team_abbr ??
              p.opponent_abbr ??
              p.opponent_team ??
              p.opponent
          ) ?? undefined,

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

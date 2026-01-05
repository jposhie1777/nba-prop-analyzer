import { LiveGame, PeriodScore } from "@/types/live";

/**
 * Adapt raw API payload into UI-safe LiveGame[]
 * This is the ONLY place backend shapes are handled.
 */
export function adaptLiveGames(apiGames: any[]): LiveGame[] {
  return apiGames.map((g): LiveGame => ({
    gameId: String(g.game_id ?? g.id),

    status: mapStatus(g.status),
    clock: g.clock ?? undefined,
    period: g.period ?? undefined,

    home: {
      team: g.home?.name ?? g.home_team,
      abbrev: g.home?.abbrev ?? g.home_abbrev,
      score: numberOrUndefined(g.home_score),
    },

    away: {
      team: g.away?.name ?? g.away_team,
      abbrev: g.away?.abbrev ?? g.away_abbrev,
      score: numberOrUndefined(g.away_score),
    },

    lineScore: adaptLineScore(g.line_score),

    odds: adaptOdds(g.odds),
  }));
}

/* =============================
   Helpers
============================= */

function mapStatus(status: string): "pre" | "live" | "final" {
  if (!status) return "pre";
  if (["in_progress", "live"].includes(status)) return "live";
  if (["final", "completed"].includes(status)) return "final";
  return "pre";
}

function numberOrUndefined(val: any): number | undefined {
  return typeof val === "number" ? val : undefined;
}

/* =============================
   Line score
============================= */

function adaptLineScore(raw: any[]): PeriodScore[] | undefined {
  if (!Array.isArray(raw)) return undefined;

  return raw.map((p) => ({
    label: p.label,
    home: p.home ?? null,
    away: p.away ?? null,
  }));
}

/* =============================
   Odds
============================= */

function adaptOdds(raw: any): LiveGame["odds"] {
  if (!raw) return [];

  return [
    raw.fanduel && {
      book: "fanduel",
      moneyline: {
        home: raw.fanduel.ml_home,
        away: raw.fanduel.ml_away,
      },
      spread: {
        home: raw.fanduel.spread_home,
        away: raw.fanduel.spread_away,
        line: raw.fanduel.spread,
      },
      total: {
        over: raw.fanduel.over,
        under: raw.fanduel.under,
        line: raw.fanduel.total,
      },
    },
    raw.draftkings && {
      book: "draftkings",
      moneyline: {
        home: raw.draftkings.ml_home,
        away: raw.draftkings.ml_away,
      },
      spread: {
        home: raw.draftkings.spread_home,
        away: raw.draftkings.spread_away,
        line: raw.draftkings.spread,
      },
      total: {
        over: raw.draftkings.over,
        under: raw.draftkings.under,
        line: raw.draftkings.total,
      },
    },
  ].filter(Boolean) as LiveGame["odds"];
}

import { LiveGame, PeriodScore } from "@/types/live";

/**
 * Adapt raw LIVE API payload into UI-safe LiveGame[]
 *
 * BACKEND CONTRACT (Cloud Run):
 * {
 *   game_id: number
 *   home_team: string
 *   away_team: string
 *   home_score: number | null
 *   away_score: number | null
 *   home_q: (number | null)[]
 *   away_q: (number | null)[]
 *   period: string | null
 *   clock: string | null
 * }
 *
 * This file is the ONLY place backend shapes are handled.
 */
export function adaptLiveGames(apiGames: any[]): LiveGame[] {
  console.log(
    "🔧 adaptLiveGames input:",
    Array.isArray(apiGames) ? apiGames.length : apiGames
  );

  if (!Array.isArray(apiGames)) return [];

  const out = apiGames.map((g): LiveGame => ({
    gameId: String(g.game_id),
    status: "live",
    clock: g.clock ?? undefined,
    period: g.period ?? undefined,
    home: {
      team: g.home_team,
      abbrev: g.home_team,
      score: numberOrUndefined(g.home_score),
    },
    away: {
      team: g.away_team,
      abbrev: g.away_team,
      score: numberOrUndefined(g.away_score),
    },
    lineScore: adaptQuarterScores(g),
    odds: [],
  }));

  console.log("🔧 adaptLiveGames output:", out.length);
  return out;
}

/* =============================
   Helpers
============================= */

function numberOrUndefined(val: any): number | undefined {
  return typeof val === "number" ? val : undefined;
}

/* =============================
   Quarter / Line score
============================= */

function adaptQuarterScores(g: any): PeriodScore[] | undefined {
  if (!Array.isArray(g.home_q) || !Array.isArray(g.away_q)) {
    return undefined;
  }

  const labels = ["Q1", "Q2", "Q3", "Q4"];

  return labels.map((label, i) => ({
    label,
    home: g.home_q[i] ?? null,
    away: g.away_q[i] ?? null,
  }));
}

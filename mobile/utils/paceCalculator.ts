// utils/paceCalculator.ts

export type RiskLevel = "on_track" | "at_risk" | "danger" | "hit" | "lost";

export type PaceResult = {
  gameProgress: number; // 0-1 (0 = start, 1 = end)
  expectedStat: number; // what they'd need at this pace to hit
  currentPace: number; // projected final stat at current rate
  riskLevel: RiskLevel;
  paceRatio: number; // current / expected (>1 = ahead, <1 = behind)
};

/**
 * Calculate game progress as a decimal (0-1).
 * NBA: 4 quarters x 12 minutes = 48 minutes total.
 * OT: 5 minutes each.
 */
export function calcGameProgress(
  period: number | null | undefined,
  clock: string | null | undefined
): number {
  if (!period) return 0;

  // Parse clock "MM:SS" or "M:SS"
  let clockSeconds = 0;
  if (clock) {
    const parts = clock.split(":");
    if (parts.length === 2) {
      const mins = parseInt(parts[0], 10) || 0;
      const secs = parseInt(parts[1], 10) || 0;
      clockSeconds = mins * 60 + secs;
    }
  }

  // Regular time: 4 quarters x 12 min = 48 min = 2880 sec
  const QUARTER_SEC = 12 * 60; // 720
  const REGULATION_SEC = 4 * QUARTER_SEC; // 2880
  const OT_SEC = 5 * 60; // 300

  let elapsedSec: number;

  if (period <= 4) {
    // Regulation: elapsed = completed quarters + time into current quarter
    const completedQuarters = period - 1;
    const timeIntoQuarter = QUARTER_SEC - clockSeconds;
    elapsedSec = completedQuarters * QUARTER_SEC + timeIntoQuarter;
  } else {
    // Overtime: all regulation + OT periods
    const otPeriod = period - 4;
    const completedOT = otPeriod - 1;
    const timeIntoOT = OT_SEC - clockSeconds;
    elapsedSec =
      REGULATION_SEC + completedOT * OT_SEC + timeIntoOT;
  }

  // For progress calculation, assume regulation (48 min)
  // OT can push past 1.0 which is fine
  return Math.min(1, elapsedSec / REGULATION_SEC);
}

/**
 * Calculate pace and risk level for a parlay leg.
 */
export function calcPace(params: {
  current: number | undefined;
  line: number;
  side: "over" | "under";
  period: number | null | undefined;
  clock: string | null | undefined;
  gameStatus: "pregame" | "live" | "final" | undefined;
}): PaceResult {
  const { current, line, side, period, clock, gameStatus } = params;

  // Handle final games
  if (gameStatus === "final") {
    const stat = current ?? 0;
    const hit =
      side === "over" ? stat > line : stat < line;
    return {
      gameProgress: 1,
      expectedStat: line,
      currentPace: stat,
      riskLevel: hit ? "hit" : "lost",
      paceRatio: stat / line,
    };
  }

  // Handle pregame or no data
  if (gameStatus === "pregame" || current === undefined) {
    return {
      gameProgress: 0,
      expectedStat: 0,
      currentPace: 0,
      riskLevel: "on_track",
      paceRatio: 1,
    };
  }

  const progress = calcGameProgress(period, clock);

  // Avoid division by zero early in game
  if (progress < 0.05) {
    return {
      gameProgress: progress,
      expectedStat: 0,
      currentPace: 0,
      riskLevel: "on_track",
      paceRatio: 1,
    };
  }

  // Expected stat at this point to be on pace
  const expectedStat = line * progress;

  // Projected final stat at current rate
  const currentPace = current / progress;

  // Pace ratio: >1 means ahead, <1 means behind
  const paceRatio = current / Math.max(expectedStat, 0.1);

  // Determine risk level
  let riskLevel: RiskLevel;

  if (side === "over") {
    // OVER bet: need current to be high
    if (current > line) {
      // Already hit the line
      riskLevel = "on_track";
    } else if (paceRatio >= 0.85) {
      // Within 15% of expected pace
      riskLevel = "on_track";
    } else if (paceRatio >= 0.6) {
      // 60-85% of expected pace - at risk
      riskLevel = "at_risk";
    } else {
      // Below 60% of pace - danger
      riskLevel = "danger";
    }
  } else {
    // UNDER bet: need current to be low
    if (current >= line) {
      // Already busted the line
      riskLevel = "danger";
    } else if (paceRatio <= 1.15) {
      // Not exceeding pace by more than 15%
      riskLevel = "on_track";
    } else if (paceRatio <= 1.4) {
      // 115-140% of expected - at risk
      riskLevel = "at_risk";
    } else {
      // Exceeding pace by 40%+ - danger
      riskLevel = "danger";
    }
  }

  return {
    gameProgress: progress,
    expectedStat,
    currentPace,
    riskLevel,
    paceRatio,
  };
}

/**
 * Check if a leg is at risk or worse.
 */
export function isAtRisk(riskLevel: RiskLevel): boolean {
  return riskLevel === "at_risk" || riskLevel === "danger";
}

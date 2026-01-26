// mobile/utils/parlayProgress.ts

export type ParlaySide = "over" | "under";

/**
 * Calculate progress (0 → 1) toward a leg resolving.
 * - Over bets fill left → right
 * - Under bets fill right → left
 */
export function calcLegProgress(
  current?: number,
  line?: number,
  side?: ParlaySide
): number {
  if (current == null || line == null || line <= 0) return 0;

  const raw = current / line;

  if (side === "over") {
    return clamp(raw, 0, 1);
  }

  // under bets invert
  return clamp(1 - raw, 0, 1);
}

function clamp(value: number, min: number, max: number) {
  return Math.min(Math.max(value, min), max);
}

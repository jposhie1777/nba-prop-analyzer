import type { TrackedParlaySnapshot } from "@/store/useParlayTracker";

/**
 * Returns the "betting day" for a given date.
 * NBA games can run past midnight, so the betting day
 * doesn't end until 3:00 AM ET the next calendar day.
 *
 * Example: Monday 7 PM → betting day is Monday
 *          Tuesday 2 AM → betting day is still Monday
 *          Tuesday 4 AM → betting day is Tuesday
 */
function getBettingDay(date: Date): string {
  const d = new Date(date);
  // If before 3 AM, it's still the previous betting day
  if (d.getHours() < 3) {
    d.setDate(d.getDate() - 1);
  }
  // Return YYYY-MM-DD format for easy comparison
  return d.toISOString().slice(0, 10);
}

/**
 * Determines if a parlay should be expired.
 *
 * A parlay expires when:
 * 1. It was created on a PREVIOUS betting day (regardless of leg status), OR
 * 2. All legs are final AND past 3 AM next day (existing logic)
 */
export function shouldExpireParlay(
  parlay: TrackedParlaySnapshot,
  now = new Date()
): boolean {
  const created = new Date(parlay.created_at);
  const parlayBettingDay = getBettingDay(created);
  const todayBettingDay = getBettingDay(now);

  // 1️⃣ Expire if from a previous betting day (clears old parlays)
  if (parlayBettingDay < todayBettingDay) {
    return true;
  }

  // 2️⃣ All legs final? Keep until 3 AM buffer passes
  const allFinal = parlay.legs.every(
    (l) => l.game_status === "final"
  );
  if (!allFinal) return false;

  // 3️⃣ End of betting day (3am local safety buffer)
  const expiry = new Date(created);
  expiry.setDate(expiry.getDate() + 1);
  expiry.setHours(3, 0, 0, 0); // 3:00 AM next day

  return now >= expiry;
}

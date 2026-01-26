export function shouldExpireParlay(
  parlay: TrackedParlaySnapshot,
  now = new Date()
): boolean {
  // 1️⃣ All legs final?
  const allFinal = parlay.legs.every(
    (l) => l.game_status === "final"
  );
  if (!allFinal) return false;

  // 2️⃣ End of betting day (3am local safety buffer)
  const created = new Date(parlay.created_at);
  const expiry = new Date(created);
  expiry.setDate(expiry.getDate() + 1);
  expiry.setHours(3, 0, 0, 0); // 3:00 AM next day

  return now >= expiry;
}

import { SavedBet } from "@/store/useBetsStore";
import { formatBetsForGambly } from "./gambly";

const DISCORD_WEBHOOK =
  process.env.EXPO_PUBLIC_DISCORD_GAMBLY_WEBHOOK!;

export async function sendBetsToDiscord(bets: SavedBet[]) {
  const content = [
    `📤 **Pulse Bets (${bets.length})**`,
    "",
    formatBetsForGambly(bets),
  ].join("\n");

  const res = await fetch(DISCORD_WEBHOOK, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ content }),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(
      `Discord webhook failed (${res.status}): ${text}`
    );
  }
}
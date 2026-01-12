// lib/export/sendToDiscord.ts
import { SavedBet } from "@/store/useBetsStore";
import { formatBetsForGambly } from "./gambly";

const DISCORD_WEBHOOK =
  process.env.EXPO_PUBLIC_DISCORD_GAMBLY_WEBHOOK!;

// ✅ REAL Gambly Bot ID
const GAMBLy_BOT_ID = "1338973806383071392";

export async function sendBetsToDiscord(bets: SavedBet[]) {
  if (!bets.length) return;

  const content = [
    "📤 **Pulse Bets**",
    `<@${GAMBLy_BOT_ID}>`, // 👈 THIS is the real mention
    "",
    formatBetsForGambly(bets),
  ].join("\n");

  const res = await fetch(DISCORD_WEBHOOK, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      content,
      // 🔑 REQUIRED or Discord will silently ignore the mention
      allowed_mentions: {
        users: [GAMBLy_BOT_ID],
      },
    }),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(
      `Discord webhook failed (${res.status}): ${text}`
    );
  }
}
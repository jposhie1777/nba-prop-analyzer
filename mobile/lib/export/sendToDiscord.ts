// lib/export/sendToDiscord.ts
import { SavedBet } from "@/store/useBetsStore";
import { formatBetsForGambly } from "./gambly";

const DISCORD_WEBHOOK =
  process.env.EXPO_PUBLIC_DISCORD_GAMBLY_WEBHOOK!;

// ✅ Playbook Bot ID
const PLAYBOOK_BOT_ID = "1408438245594763375";

export async function sendBetsToDiscord(bets: SavedBet[]) {
  if (!bets.length) return;

  const content = [
    `<@${PLAYBOOK_BOT_ID}>`,
    "",
    "📤 **Pulse Bets**",
    "",
    formatBetsForGambly(bets),
  ].join("\n");

  const res = await fetch(DISCORD_WEBHOOK, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      content,
      // 🔑 REQUIRED — otherwise the mention is ignored
      allowed_mentions: {
        users: [PLAYBOOK_BOT_ID],
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
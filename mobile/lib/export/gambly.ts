// lib/export/gambly.ts
import { SavedBet } from "@/store/useBetsStore";

export function formatBetsForGambly(bets: SavedBet[]): string {
  if (!bets.length) return "";

  return bets
    .map((b) => {
      // -----------------------------
      // CORE FIELDS
      // -----------------------------
      const league = "NBA";
      const gameId = b.gameId ?? "—";
      const player = b.playerName ?? "GAME";

      // -----------------------------
      // MARKET / OUTCOME
      // -----------------------------
      const market = b.marketKey ?? "PROP";
      const outcome = b.outcome ?? "OVER";

      // -----------------------------
      // LINE
      // -----------------------------
      const line =
        b.line !== undefined && b.line !== null ? b.line : "—";

      // -----------------------------
      // ODDS
      // -----------------------------
      const odds =
        typeof b.odds === "number"
          ? b.odds > 0
            ? `+${b.odds}`
            : `${b.odds}`
          : "—";

      // -----------------------------
      // BOOK
      // -----------------------------
      const book = b.book?.toUpperCase() ?? "SAVED";

      // -----------------------------
      // FINAL OUTPUT (Discord / Gambly)
      // -----------------------------
      return [
        league,
        gameId,
        player,
        market,
        outcome,
        line,
        odds,
        book,
      ].join(" | ");
    })
    .join("\n");
}
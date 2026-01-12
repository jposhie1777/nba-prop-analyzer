// lib/export/gambly.ts
import { SavedBet } from "@/store/useBetsStore";

type PlayerNameLookup = (playerId?: string) => string | undefined;

export function formatBetsForGambly(
  bets: SavedBet[],
  getPlayerName?: PlayerNameLookup
): string {
  if (!bets.length) return "";

  return bets
    .map((b) => {
      // -----------------------------
      // PLAYER
      // -----------------------------
      const player =
        getPlayerName?.(b.playerId) ??
        (b.playerId ? `Player ${b.playerId}` : "GAME");

      // -----------------------------
      // MARKET / OUTCOME
      // -----------------------------
      const market = b.marketKey ?? "PROP";
      const outcome = b.outcome ?? "OVER";

      // -----------------------------
      // LINE / ODDS
      // -----------------------------
      const line =
        b.line !== undefined && b.line !== null ? b.line : "—";

      const odds =
        typeof b.odds === "number"
          ? b.odds > 0
            ? `+${b.odds}`
            : String(b.odds)
          : "—";

      // -----------------------------
      // BOOK
      // -----------------------------
      const book = b.book ? b.book.toUpperCase() : "SAVED";

      // -----------------------------
      // FINAL FORMAT (GAMBLY FRIENDLY)
      // -----------------------------
      return [
        "NBA",
        b.gameId ?? "—",
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
// lib/export/gambly.ts
import { SavedBet } from "@/store/useBetsStore";

type PlayerNameLookup = (playerId?: string) => string | undefined;

export function formatBetsForGambly(
  bets: SavedBet[],
  getPlayerName?: PlayerNameLookup
): string {
  return bets
    .map((b) => {
      const player =
        getPlayerName?.(b.playerId) ??
        (b.playerId ? `Player ${b.playerId}` : "GAME");

      return [
        "NBA",
        b.gameId,
        player,
        b.marketKey,
        b.outcome,
        b.line,
        b.odds > 0 ? `+${b.odds}` : b.odds,
        b.book.toUpperCase(),
      ].join(" | ");
    })
    .join("\n");
}
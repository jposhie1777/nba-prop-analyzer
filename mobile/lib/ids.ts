//  /lib/ids.ts
import { GameId, PlayerId, MarketKey, Outcome } from "@/types/betting";

export function makeSelectionId(params: {
  gameId: GameId;
  playerId?: PlayerId;
  marketKey: MarketKey;
  outcome: Outcome;
  line: number;
}) {
  const { gameId, playerId, marketKey, outcome, line } = params;

  return [
    gameId,
    playerId ?? "GAME",
    marketKey,
    outcome,
    line,
  ].join(":");
}
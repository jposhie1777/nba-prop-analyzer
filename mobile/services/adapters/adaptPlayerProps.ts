//. /services/adapters/adaptPlayerProps
import { PlayerPropMarket, MarketKey } from "@/types/betting";
import { makeSelectionId } from "@/lib/ids";

type RawLivePlayerProp = {
  player_id: number;
  market: MarketKey;
  line: number;
  book: "draftkings" | "fanduel";
  over: number;
  under: number;
};

type RawResponse = {
  game_id: number;
  updated_at: number | string | null;
  props: RawLivePlayerProp[];
};

export function adaptPlayerProps(
  raw: RawResponse,
): {
  gameId: string;
  playerId: string;
  markets: PlayerPropMarket[];
}[] {
  const gameId = String(raw.game_id);
  const updatedAt = Date.now();

  // ------------------------------
  // Group by player → market
  // ------------------------------
  const byPlayer: Record<
    string,
    Record<MarketKey, MarketSelection[]>
  > = {};

  for (const row of raw.props) {
    const playerId = String(row.player_id);
    const marketKey = row.market;

    if (!byPlayer[playerId]) {
      byPlayer[playerId] = {};
    }

    if (!byPlayer[playerId][marketKey]) {
      byPlayer[playerId][marketKey] = [];
    }

    // OVER
    if (row.over != null) {
      byPlayer[playerId][marketKey].push({
        selectionId: makeSelectionId({
          gameId,
          playerId,
          marketKey,
          outcome: "OVER",
          line: row.line,
        }),
        gameId,
        playerId,
        marketKey,
        outcome: "OVER",
        line: row.line,
        best: {
          book: row.book,
          odds: row.over,
          updatedAt,
        },
      });
    }

    // UNDER
    if (row.under != null) {
      byPlayer[playerId][marketKey].push({
        selectionId: makeSelectionId({
          gameId,
          playerId,
          marketKey,
          outcome: "UNDER",
          line: row.line,
        }),
        gameId,
        playerId,
        marketKey,
        outcome: "UNDER",
        line: row.line,
        best: {
          book: row.book,
          odds: row.under,
          updatedAt,
        },
      });
    }
  }

  // ------------------------------
  // Build PlayerPropMarket arrays
  // ------------------------------
  return Object.entries(byPlayer).map(([playerId, markets]) => {
    const marketList: PlayerPropMarket[] = Object.entries(markets).map(
      ([marketKey, selections]) => ({
        marketKey: marketKey as MarketKey,
        selections: selections.sort((a, b) => {
          // stable order: line ASC, OVER first
          if (a.line !== b.line) return a.line - b.line;
          return a.outcome === "OVER" ? -1 : 1;
        }),
        updatedAt,
      })
    );

    return {
      gameId,
      playerId,
      markets: marketList,
    };
  });
}
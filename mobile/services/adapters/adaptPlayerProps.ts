// services/adapters/adaptPlayerProps.ts

/**
 * ADAPTER CONTRACT
 * ----------------
 * - Accept raw backend payloads
 * - Output betting-domain types ONLY
 * - Generate stable selectionIds
 * - Sort deterministically
 * - NEVER touch UI or stores
 */

import {
  PlayerPropMarket,
  MarketKey,
  MarketSelection,
} from "@/types/betting";
import { makeSelectionId } from "@/lib/ids";

type RawPropRow = {
  player_id: number;
  market: MarketKey;
  line: number;
  book: string;
  over: number | null;
  under: number | null;
};

type RawPlayerPropsResponse = {
  game_id: number;
  updated_at?: string | null;
  props: RawPropRow[];
};

export function adaptPlayerProps(
  raw: RawPlayerPropsResponse
): Array<{
  gameId: string;
  playerId: string;
  markets: PlayerPropMarket[];
}> {
  const gameId = String(raw.game_id);
  const updatedAt = Date.now();

  // playerId -> marketKey -> selections
  const byPlayer: Record<
    string,
    Record<MarketKey, MarketSelection[]>
  > = {};

  for (const row of raw.props) {
    const playerId = String(row.player_id);
    const marketKey = row.market;

    if (!byPlayer[playerId]) byPlayer[playerId] = {};
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

  // Build markets per player
  return Object.entries(byPlayer).map(([playerId, markets]) => {
    const marketList: PlayerPropMarket[] = Object.entries(markets).map(
      ([marketKey, selections]) => ({
        marketKey: marketKey as MarketKey,
        selections: selections.sort((a, b) => {
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
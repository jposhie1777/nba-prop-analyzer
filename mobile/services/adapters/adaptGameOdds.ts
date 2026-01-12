// /services/adapters/adaptGameOdds.ts

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
  GameOdds,
  MarketSelection,
} from "@/types/betting";
import { makeSelectionId } from "@/lib/ids";

type RawGameOddsRow = {
  book: string;

  spread_home?: string | number | null;
  spread_home_odds?: number | null;
  spread_away?: string | number | null;
  spread_away_odds?: number | null;

  total?: string | number | null;
  total_over_odds?: number | null;
  total_under_odds?: number | null;

  moneyline_home_odds?: number | null;
  moneyline_away_odds?: number | null;

  updated_at?: string | null;
};

type RawGameOddsResponse = {
  game_id: number;
  odds: RawGameOddsRow[];
};

function toNumber(val: string | number | null | undefined): number | null {
  if (val == null) return null;
  const n = Number(val);
  return Number.isFinite(n) ? n : null;
}

export function adaptGameOdds(
  raw: RawGameOddsResponse,
): GameOdds {
  const gameId = String(raw.game_id);
  const updatedAt = Date.now();

  const spreadSelections: MarketSelection[] = [];
  const totalSelections: MarketSelection[] = [];

  for (const row of raw.odds) {
    const book = row.book;

    // ---------------------------
    // SPREAD
    // ---------------------------
    const spreadHome = toNumber(row.spread_home);
    const spreadAway = toNumber(row.spread_away);

    if (spreadHome != null && row.spread_home_odds != null) {
      spreadSelections.push({
        selectionId: makeSelectionId({
          gameId,
          marketKey: "SPREAD",
          outcome: "HOME",
          line: spreadHome,
        }),
        gameId,
        marketKey: "SPREAD",
        outcome: "HOME",
        line: spreadHome,
        best: {
          book,
          odds: row.spread_home_odds,
          updatedAt,
        },
      });
    }

    if (spreadAway != null && row.spread_away_odds != null) {
      spreadSelections.push({
        selectionId: makeSelectionId({
          gameId,
          marketKey: "SPREAD",
          outcome: "AWAY",
          line: spreadAway,
        }),
        gameId,
        marketKey: "SPREAD",
        outcome: "AWAY",
        line: spreadAway,
        best: {
          book,
          odds: row.spread_away_odds,
          updatedAt,
        },
      });
    }

    // ---------------------------
    // TOTAL
    // ---------------------------
    const total = toNumber(row.total);

    if (total != null && row.total_over_odds != null) {
      totalSelections.push({
        selectionId: makeSelectionId({
          gameId,
          marketKey: "TOTAL",
          outcome: "OVER",
          line: total,
        }),
        gameId,
        marketKey: "TOTAL",
        outcome: "OVER",
        line: total,
        best: {
          book,
          odds: row.total_over_odds,
          updatedAt,
        },
      });
    }

    if (total != null && row.total_under_odds != null) {
      totalSelections.push({
        selectionId: makeSelectionId({
          gameId,
          marketKey: "TOTAL",
          outcome: "UNDER",
          line: total,
        }),
        gameId,
        marketKey: "TOTAL",
        outcome: "UNDER",
        line: total,
        best: {
          book,
          odds: row.total_under_odds,
          updatedAt,
        },
      });
    }
  }

  return {
    gameId,
    spread: spreadSelections.length ? spreadSelections : undefined,
    total: totalSelections.length ? totalSelections : undefined,
    updatedAt,
  };
}
// services/adapters/gameOddsToBet.ts
import { SavedBet } from "@/store/useSavedBets";

/* ======================================================
   GAME SPREAD
====================================================== */

export function adaptGameSpreadToBet({
  gameId,
  teamAbbr,
  opponentAbbr,
  line,
  odds,
  bookmaker,
  side, // "home" | "away"
}: {
  gameId: string;
  teamAbbr: string;
  opponentAbbr: string;
  line: number;
  odds: number | null;
  bookmaker: string;
  side: "home" | "away";
}): SavedBet {
  return {
    // 🔐 UNIQUE + COLLISION-SAFE
    id: `game:${gameId}:spread:${side}:${teamAbbr}:${line}:${bookmaker}`,

    betType: "game",

    gameId: Number(gameId),
    bookmaker,
    odds: odds ?? undefined,

    side,
    line,

    // 🆕 game-only display fields
    teams: `${teamAbbr} vs ${opponentAbbr}`,
  };
}

/* ======================================================
   GAME TOTAL
====================================================== */

export function adaptGameTotalToBet({
  gameId,
  line,
  odds,
  side, // "over" | "under"
  bookmaker,
  teams,
}: {
  gameId: string;
  line: number;
  odds: number | null;
  side: "over" | "under";
  bookmaker: string;
  teams: string;
}): SavedBet {
  return {
    // 🔐 UNIQUE + COLLISION-SAFE
    id: `game:${gameId}:total:${side}:${line}:${bookmaker}`,

    betType: "game",

    gameId: Number(gameId),
    bookmaker,
    odds: odds ?? undefined,

    side,
    line,

    teams,
  };
}
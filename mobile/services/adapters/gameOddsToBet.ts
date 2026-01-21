// services/adapters/gameOddsToBet.ts
import { Bet } from "@/types/bet";

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
}): Bet {
  return {
    // 🔐 UNIQUE + COLLISION-SAFE
    id: `game:${gameId}:spread:${side}:${teamAbbr}:${line}:${bookmaker}`,

    betType: "game",
    type: "game_spread",

    gameId,
    bookmaker,

    side,
    line,
    odds,

    teamAbbr,
    opponentAbbr,

    display: {
      title: `${teamAbbr} ${line}`,
      subtitle: `${teamAbbr} vs ${opponentAbbr} • Spread`,
    },
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
}): Bet {
  return {
    // 🔐 UNIQUE + COLLISION-SAFE
    id: `game:${gameId}:total:${side}:${line}:${bookmaker}`,

    betType: "game",
    type: "game_total",

    gameId,
    bookmaker,

    side,
    line,
    odds,

    display: {
      title: `${side === "over" ? "O" : "U"} ${line}`,
      subtitle: `${teams} • Total`,
    },
  };
}
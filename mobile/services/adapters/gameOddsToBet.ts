// services/adapters/gameOddsToBet.ts
import { Bet } from "@/types/bet";

export function adaptGameSpreadToBet({
  gameId,
  teamAbbr,
  opponentAbbr,
  line,
  odds,
  bookmaker,
}: any): Bet {
  return {
    id: `spread:${gameId}:${teamAbbr}:${line}:${bookmaker}`,

    type: "game_spread",
    gameId,
    bookmaker,

    side: "home", // or away
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

export function adaptGameTotalToBet({
  gameId,
  line,
  odds,
  side,
  bookmaker,
  teams,
}: any): Bet {
  return {
    id: `total:${gameId}:${side}:${line}:${bookmaker}`,

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
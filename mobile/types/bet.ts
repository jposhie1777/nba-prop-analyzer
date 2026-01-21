// types/bet.ts
export type BetType =
  | "player_prop"
  | "game_spread"
  | "game_total"
  | "game_moneyline";

export type BetSource = "draftkings" | "fanduel";

export type Bet = {
  id: string;                 // stable unique id
  type: BetType;

  gameId: number;
  bookmaker: BetSource;

  odds: number;               // -110, +120, etc
  line?: number | null;       // spread / total
  side: "over" | "under" | "home" | "away";

  teamAbbr?: string;
  opponentAbbr?: string;

  display: {
    title: string;            // "CHI -8.5"
    subtitle: string;         // "vs LAC • Spread"
  };

  meta?: Record<string, any>; // future-proof (quarter, alt, etc)
};
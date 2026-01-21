export type BetType = "player" | "game";

export type Bet = {
  id: string;
  betType: BetType;

  // shared
  odds: number | null;
  bookmaker: string;

  // display
  label: string;

  // routing metadata
  gameId?: string;

  // player-only
  playerId?: number;
  player?: string;
  market?: string;
  line?: number;
  side?: string;

  // game-only
  teams?: string;
};
//   /types/betting.ts
// ===============================
// Core IDs
// ===============================
export type GameId = string;
export type PlayerId = string;

export type MarketKey =
  | "PTS" | "REB" | "AST"
  | "PR" | "RA" | "PA" | "PRA"
  | "3PM"
  | "BLK" | "STL"
  | "TO"
  | "SPREAD"
  | "TOTAL";

export type Outcome =
  | "OVER"
  | "UNDER"
  | "HOME"
  | "AWAY";

// ===============================
// Game
// ===============================
export type GameStatus = "UPCOMING" | "LIVE" | "FINAL";

export type LiveGame = {
  gameId: GameId;
  startTimeEt: string | null;
  status: GameStatus;

  home: {
    team: string;
    score: number;
  };

  away: {
    team: string;
    score: number;
  };

  period: number | null;
  clock: string | null;

  updatedAt: number;
};

// ===============================
// Odds / Prices
// ===============================
export type BookPrice = {
  book: string;
  odds: number;
  updatedAt: number;
};

// ===============================
// Market Selection (bettable unit)
// ===============================
export type MarketSelection = {
  selectionId: string; // STABLE
  gameId: GameId;
  playerId?: PlayerId;

  marketKey: MarketKey;
  outcome: Outcome;
  line: number;

  best: BookPrice;

  // future
  ev?: number;
  confidence?: number;
};

// ===============================
// Game Odds
// ===============================
export type GameOdds = {
  gameId: GameId;
  spread?: MarketSelection[];
  total?: MarketSelection[];
  updatedAt: number;
};

// ===============================
// Player Live Stats
// ===============================
export type LivePlayer = {
  gameId: GameId;
  playerId: PlayerId;

  name: string;
  team: string;
  opponent: string;

  minutes: number | null;

  pts: number;
  reb: number;
  ast: number;
  stl: number;
  blk: number;
  tov: number;

  fg: [number, number];
  fg3: [number, number];
  ft: [number, number];

  updatedAt: number;
};

// ===============================
// Player Prop Markets
// ===============================
export type PlayerPropMarket = {
  marketKey: MarketKey;
  selections: MarketSelection[]; // horizontal rail
  updatedAt: number;
};
export type TrackedParlayLeg = {
  legId: string;

  // 🔑 Identity
  player_id: number;     // REQUIRED for live joins
  game_id?: number;      // optional but useful later
  team?: string;

  // Display
  player?: string;

  // Market
  market: string;        // "PTS", "REB", "AST", "3PM"
  line: number;
  side: "over" | "under";

  // Odds
  odds: number;

  // Live
  current?: number;
  status?: "pending" | "winning" | "losing" | "pushed";
  isFinal?: boolean;
};

export type TrackedParlay = {
  parlayId: string;

  sportsbook: "fanduel" | "draftkings";
  stake: number;
  payout: number;
  odds: number;

  createdAt: string;

  legs: TrackedParlayLeg[];
};
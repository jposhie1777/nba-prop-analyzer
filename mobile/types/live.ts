// types/live.ts

/* =============================
   Period / quarter score
============================= */
export type PeriodScore = {
  label: string;          // "Q1", "Q2", "Q3", "Q4", "OT"
  home: number | null;
  away: number | null;
};

/* =============================
   Odds primitives
============================= */
export type Moneyline = {
  home: number;
  away: number;
};

export type Spread = {
  home: number;
  away: number;
  line: number;
};

export type Total = {
  over: number;
  under: number;
  line: number;
};

/* =============================
   Sportsbook odds
============================= */
export type BookOdds = {
  book: "fanduel" | "draftkings";
  moneyline: Moneyline;
  spread: Spread;
  total: Total;
};

/* =============================
   Live game (UI contract)
============================= */
export type LiveGame = {
  gameId: string;

  status: "pre" | "live" | "final";
  clock?: string;
  period?: string;

  home: {
    team: string;
    abbrev: string;
    score?: number;
  };

  away: {
    team: string;
    abbrev: string;
    score?: number;
  };

  /* =============================
     Line score (quarters / OT)
  ============================== */
  lineScore?: PeriodScore[];

  odds: BookOdds[];
};

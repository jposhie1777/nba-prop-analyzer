export type Prop = {
  id: string;

  player: string;
  market: string;
  line: number;
  odds: number;

  hitRate: number; // 0–1
  edge: number;    // 0–1

  home: string;
  away: string;

  confidence?: number;
};

export const MOCK_PROPS: Prop[] = [
  {
    id: "tatum_pts",
    player: "Jayson Tatum",
    market: "Points",
    line: 28.5,
    odds: -110,
    hitRate: 0.78,
    edge: 0.12,
    home: "BOS",
    away: "NYK",
    confidence: 78,
  },
  {
    id: "bridges_pts",
    player: "Miles Bridges",
    market: "Points",
    line: 18.5,
    odds: -105,
    hitRate: 0.82,
    edge: 0.11,
    home: "CHA",
    away: "MIL",
    confidence: 78,
  },
  {
    id: "davis_blk",
    player: "Anthony Davis",
    market: "Blocks",
    line: 2.5,
    odds: +120,
    hitRate: 0.69,
    edge: 0.07,
    home: "PHX",
    away: "LAL",
    confidence: 65,
  },
];
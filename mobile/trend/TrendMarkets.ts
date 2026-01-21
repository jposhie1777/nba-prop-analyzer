export type TrendMarket = {
  key: string;
  label: string;
};

export const TREND_MARKETS: TrendMarket[] = [
  { key: "pts", label: "PTS" },
  { key: "reb", label: "REB" },
  { key: "ast", label: "AST" },
  { key: "pra", label: "PRA" },
  { key: "fg3m", label: "3PM" },
  { key: "fga", label: "FGA" },
  { key: "fta", label: "FTA" },
  { key: "turnover", label: "TO" },
];
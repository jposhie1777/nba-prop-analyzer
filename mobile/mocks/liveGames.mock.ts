// mocks/live.ts
import { LiveGame } from "@/types/live";

export const MOCK_LIVE_GAMES: LiveGame[] = [
  {
    gameId: "nba-001",

    status: "live",
    clock: "7:42",
    period: "Q3",

    home: {
      team: "Boston Celtics",
      abbrev: "BOS",
      score: 76,
    },

    away: {
      team: "New York Knicks",
      abbrev: "NYK",
      score: 71,
    },

    lineScore: [
      { label: "Q1", home: 22, away: 18 },
      { label: "Q2", home: 28, away: 25 },
      { label: "Q3", home: 26, away: 28 },
      { label: "Q4", home: null, away: null },
    ],

    odds: [
      {
        book: "fanduel",
        moneyline: {
          home: -135,
          away: +115,
        },
        spread: {
          home: -4.5,
          away: +4.5,
          line: -4.5,
        },
        total: {
          over: -110,
          under: -110,
          line: 224.5,
        },
      },
      {
        book: "draftkings",
        moneyline: {
          home: -140,
          away: +120,
        },
        spread: {
          home: -5,
          away: +5,
          line: -5,
        },
        total: {
          over: -108,
          under: -112,
          line: 225,
        },
      },
    ],
  },
];

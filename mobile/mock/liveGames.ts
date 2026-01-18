// mock/liveGames.ts
import { LiveGame } from "@/types/live";

export const MOCK_LIVE_GAMES: LiveGame[] = [
  {
    game_id: 999001,
    home_team_abbr: "DAL",
    away_team_abbr: "PHX",
    state: "LIVE",
    period: "Q3",
    clock: "6:42",
    home_score: 72,
    away_score: 69,
  },
];

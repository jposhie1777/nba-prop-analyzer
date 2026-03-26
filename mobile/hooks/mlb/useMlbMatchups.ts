import { useMemo } from "react";

import { useEplQuery } from "@/hooks/epl/useEplQuery";

export type MlbUpcomingGame = {
  game_pk: number;
  home_team?: string | null;
  away_team?: string | null;
  home_team_id?: number | null;
  away_team_id?: number | null;
  start_time_utc?: string | null;
  venue_name?: string | null;
  home_pitcher_name?: string | null;
  away_pitcher_name?: string | null;
  has_model_data?: boolean;
  picks_count?: number;
  top_score?: number | null;
  top_grade?: "IDEAL" | "FAVORABLE" | "AVERAGE" | "AVOID" | null;
  // Weather
  weather_indicator?: "Green" | "Yellow" | "Red" | string | null;
  game_temp?: number | null;
  wind_speed?: number | null;
  wind_dir?: number | null;
  precip_prob?: number | null;
  conditions?: string | null;
  ballpark_name?: string | null;
  roof_type?: string | null;
  weather_note?: string | null;
  // Odds
  home_moneyline?: number | null;
  away_moneyline?: number | null;
  over_under?: number | null;
};

export type MlbPitcherSplit = {
  ip?: number | null;
  home_runs?: number | null;
  hr_per_9?: number | null;
  barrel_pct?: number | null;
  hard_hit_pct?: number | null;
  fb_pct?: number | null;
  hr_fb_pct?: number | null;
  whip?: number | null;
  woba?: number | null;
};

export type MlbBatterPick = {
  batter_id?: number | null;
  batter_name?: string | null;
  bat_side?: string | null;
  score?: number | null;
  grade?: string | null;
  why?: string | null;
  flags?: string[];
  iso?: number | null;
  slg?: number | null;
  l15_ev?: number | null;
  l15_barrel_pct?: number | null;
  season_ev?: number | null;
  season_barrel_pct?: number | null;
  l15_hard_hit_pct?: number | null;
  hr_fb_pct?: number | null;
  p_hr9_vs_hand?: number | null;
  p_hr_fb_pct?: number | null;
  p_barrel_pct?: number | null;
  p_fb_pct?: number | null;
  p_hard_hit_pct?: number | null;
  p_iso_allowed?: number | null;
  weather_indicator?: string | null;
  game_temp?: number | null;
  wind_speed?: number | null;
  wind_dir?: number | null;
  wind_direction_label?: string | null;
  precip_prob?: number | null;
  ballpark_name?: string | null;
  roof_type?: string | null;
  weather_note?: string | null;
  home_moneyline?: number | null;
  away_moneyline?: number | null;
  over_under?: number | null;
  hr_odds_best_price?: number | null;
  hr_odds_best_book?: string | null;
  deep_link_desktop?: string | null;
  deep_link_ios?: string | null;
};

export type MlbPitcherGroup = {
  pitcher_id: number;
  pitcher_name?: string | null;
  pitcher_hand?: string | null;
  opp_team_id?: number | null;
  offense_team?: string | null;
  splits: Record<string, MlbPitcherSplit>;
  batters: MlbBatterPick[];
};

export type MlbGameWeather = {
  weather_indicator?: "Green" | "Yellow" | "Red" | string | null;
  game_temp?: number | null;
  wind_speed?: number | null;
  wind_dir?: number | null;
  wind_gust?: number | null;
  precip_prob?: number | null;
  conditions?: string | null;
  ballpark_name?: string | null;
  roof_type?: string | null;
  ballpark_azimuth?: number | null;
  weather_note?: string | null;
  home_moneyline?: number | null;
  away_moneyline?: number | null;
  over_under?: number | null;
};

export type MlbMatchupDetail = {
  game_pk: number;
  run_date: string;
  game: {
    home_team?: string | null;
    away_team?: string | null;
    start_time_utc?: string | null;
    venue_name?: string | null;
    home_pitcher_name?: string | null;
    away_pitcher_name?: string | null;
    weather?: {
      weather_indicator?: string | null;
      game_temp?: number | null;
      wind_speed?: number | null;
      wind_dir?: number | null;
      wind_direction_label?: string | null;
      precip_prob?: number | null;
      ballpark_name?: string | null;
      roof_type?: string | null;
      weather_note?: string | null;
    };
    odds?: {
      home_moneyline?: number | null;
      away_moneyline?: number | null;
      over_under?: number | null;
    };
  };
  grade_counts: {
    IDEAL: number;
    FAVORABLE: number;
    AVERAGE: number;
    AVOID: number;
  };
  pitchers: MlbPitcherGroup[];
};

export type MlbUpcomingDebug = {
  now_utc: string;
  now_et: string;
  today_et: string;
  today_schedule_count: number;
  tomorrow_et: string;
  tomorrow_schedule_count: number;
  combined_schedule_count: number;
  sample_games: {
    game_pk?: number | null;
    away_team?: string | null;
    home_team?: string | null;
    start_time_utc?: string | null;
  }[];
  bq_project?: string | null;
  hr_table: string;
  bq_today_summary_count: number;
  bq_query_ok: boolean;
  fetch_error: string | null;
};

export function useMlbUpcomingGames(limit = 30) {
  const params = useMemo(() => ({ limit }), [limit]);
  return useEplQuery<MlbUpcomingGame[]>("/mlb/matchups/upcoming", params);
}

export function useMlbUpcomingDebug() {
  return useEplQuery<MlbUpcomingDebug>("/mlb/matchups/upcoming/debug");
}

export function useMlbMatchupDetail(gamePk?: number | null) {
  return useEplQuery<MlbMatchupDetail>(
    `/mlb/matchups/${gamePk ?? 0}`,
    undefined,
    Boolean(gamePk)
  );
}

import { useEffect, useMemo, useRef } from "react";

import { useEplQuery } from "@/hooks/epl/useEplQuery";
import { API_BASE, CLOUD_API_BASE } from "@/lib/config";

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

export type MlbPitchMixRow = {
  pitch_name?: string | null;
  pitch_count?: number | null;
  pitch_pct?: number | null;
  ba?: number | null;
  woba?: number | null;
  slg?: number | null;
  iso?: number | null;
  hr?: number | null;
  k_pct?: number | null;
  whiff_pct?: number | null;
};

export type MlbBatterVsPitchRow = {
  pitch_name?: string | null;
  count?: number | null;
  pitch_pct?: number | null;
  ba?: number | null;
  woba?: number | null;
  slg?: number | null;
  iso?: number | null;
  hr?: number | null;
  ev?: number | null;
  barrel_pct?: number | null;
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
  dk_outcome_code?: string | null;
  dk_event_id?: string | null;
  fd_market_id?: string | null;
  fd_selection_id?: string | null;
  pitcher_pitch_mix?: {
    vs_lhb: MlbPitchMixRow[];
    vs_rhb: MlbPitchMixRow[];
  };
  hitter_stats_vs_pitches?: {
    vs_lhp: MlbBatterVsPitchRow[];
    vs_rhp: MlbBatterVsPitchRow[];
  };
  bvp_career?: {
    pa?: number | null;
    hits?: number | null;
    hr?: number | null;
    avg?: number | null;
    iso?: number | null;
    slg?: number | null;
    obp?: number | null;
    k_pct?: number | null;
    bb_pct?: number | null;
  } | null;
  bvp_batted_ball?: {
    profile?: {
      barrel_pct?: number | null;
      hh_pct?: number | null;
      fb_pct?: number | null;
      gb_pct?: number | null;
      ld_pct?: number | null;
      pu_pct?: number | null;
      hr_fb_pct?: number | null;
      pull_pct?: number | null;
      str_pct?: number | null;
      oppo_pct?: number | null;
      total_batted?: number | null;
    } | null;
    log?: {
      date?: string | null;
      pitch?: string | null;
      ev?: number | null;
      angle?: number | null;
      dist?: number | null;
      trajectory?: string | null;
      result?: string | null;
      hr_parks?: number | null;
    }[];
  } | null;
};

export type MlbPitcherGroup = {
  pitcher_id: number;
  pitcher_name?: string | null;
  pitcher_hand?: string | null;
  opp_team_id?: number | null;
  offense_team?: string | null;
  pitch_mix?: {
    vs_lhb: MlbPitchMixRow[];
    vs_rhb: MlbPitchMixRow[];
  };
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

// ── Pitching Props types ──────────────────────────────────────────────────

export type KPropAltLine = {
  line?: number | null;
  best_price?: number | null;
  best_book?: string | null;
  pf_rating?: number | null;
  dk_price?: number | null;
  dk_outcome_code?: string | null;
  dk_event_id?: string | null;
  dk_desktop?: string | null;
  dk_ios?: string | null;
  fd_price?: number | null;
  fd_market_id?: string | null;
  fd_selection_id?: string | null;
  fd_desktop?: string | null;
  fd_ios?: string | null;
};

export type KPropTeamK = {
  team_name?: string | null;
  splits?: Record<string, { rank?: number | null; value?: number | null }>;
};

export type KPropPitcher = {
  pitcher_id?: number | null;
  pitcher_name?: string | null;
  pitcher_hand?: string | null;
  offense_team?: string | null;
  team_code?: string | null;
  opp_team_code?: string | null;

  // Signal
  k_signal_score?: number | null;
  k_signal_rank?: number | null;
  proj_ks?: number | null;
  proj_ip?: number | null;
  proj_outs?: number | null;

  // Pitcher K stats
  ip?: number | null;
  strikeouts?: number | null;
  strikeouts_per_9?: number | null;
  k_pct?: number | null;
  strike_pct?: number | null;
  strikeout_walk_ratio?: number | null;
  batters_faced?: number | null;
  whip?: number | null;
  woba?: number | null;

  // Pitcher vs-hand
  hand_split?: string | null;
  hand_k_per_9?: number | null;
  hand_k_pct?: number | null;

  // Arsenal
  arsenal_whiff_rate?: number | null;
  arsenal_k_pct?: number | null;
  max_pitch_whiff?: number | null;
  pitch_type_count?: number | null;

  // Team K adj
  team_k_adj?: number | null;

  // Opposing team K
  opp_team_k?: KPropTeamK;

  // Standard line
  k_line?: number | null;
  k_best_price?: number | null;
  k_best_book?: string | null;
  pf_rating?: number | null;
  hit_rate_l5?: string | null;
  hit_rate_l10?: string | null;
  hit_rate_season?: string | null;
  hit_rate_vs_team?: string | null;
  avg_l10?: number | null;
  avg_home_away?: number | null;
  avg_vs_opponent?: number | null;
  streak?: number | null;

  // Edge / grade
  edge?: number | null;
  over_grade?: string | null;
  lean?: string | null;
  confidence?: string | null;

  // DK/FD for standard line
  dk_price?: number | null;
  dk_outcome_code?: string | null;
  dk_event_id?: string | null;
  dk_desktop?: string | null;
  dk_ios?: string | null;
  fd_price?: number | null;
  fd_market_id?: string | null;
  fd_selection_id?: string | null;
  fd_desktop?: string | null;
  fd_ios?: string | null;

  // Alt lines
  alt_lines?: KPropAltLine[];
};

export type MlbPitchingPropsDetail = {
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
  pitchers: KPropPitcher[];
};

export function useMlbPitchingProps(gamePk?: number | null) {
  return useEplQuery<MlbPitchingPropsDetail>(
    `/mlb/matchups/${gamePk ?? 0}/pitching-props`,
    undefined,
    Boolean(gamePk)
  );
}

// ── Batting Order Matchup types ───────────────────────────────────────────

export type BattingOrderPosition = {
  batting_order?: number | null;
  at_bats?: number | null;
  hits?: number | null;
  home_runs?: number | null;
  doubles?: number | null;
  triples?: number | null;
  rbi?: number | null;
  walks?: number | null;
  strike_outs?: number | null;
  avg?: number | null;
  obp?: number | null;
  slg?: number | null;
  ops?: number | null;
  is_weak_spot?: boolean;
  player_id?: number | null;
  player_name?: string | null;
};

export type BattingOrderPitcher = {
  pitcher_id?: number | null;
  pitcher_name?: string | null;
  pitcher_hand?: string | null;
  opp_team_id?: number | null;
  offense_team?: string | null;
  lineup_confirmed?: boolean;
  weak_spot_count?: number | null;
  positions?: BattingOrderPosition[];
};

export type MlbBattingOrderDetail = {
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
  pitchers: BattingOrderPitcher[];
};

export function useMlbBattingOrder(gamePk?: number | null) {
  return useEplQuery<MlbBattingOrderDetail>(
    `/mlb/matchups/${gamePk ?? 0}/batting-order`,
    undefined,
    Boolean(gamePk)
  );
}

// ── HR Cheat Sheet types ─────────────────────────────────────────────────

export type CheatSheetBatter = {
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
  hr_odds_best_price?: number | null;
  hr_odds_best_book?: string | null;
  dk_outcome_code?: string | null;
  dk_event_id?: string | null;
  fd_market_id?: string | null;
  fd_selection_id?: string | null;
  is_weak_spot?: boolean;
  pitcher_id?: number | null;
  pitcher_name?: string | null;
  pitcher_hand?: string | null;
  // Game context
  game_pk?: number | null;
  home_team?: string | null;
  away_team?: string | null;
  start_time_utc?: string | null;
  venue_name?: string | null;
  weather_indicator?: string | null;
  game_temp?: number | null;
};

export type MlbHrCheatSheet = {
  run_date: string;
  grades: {
    "A+": CheatSheetBatter[];
    A: CheatSheetBatter[];
    B: CheatSheetBatter[];
    C: CheatSheetBatter[];
    D: CheatSheetBatter[];
  };
  grade_counts: {
    "A+": number;
    A: number;
    B: number;
    C: number;
    D: number;
  };
};

export function useMlbHrCheatSheet() {
  return useEplQuery<MlbHrCheatSheet>("/mlb/matchups/cheat-sheet");
}

// ── Cheat-sheet batter detail (lazy loaded on expand) ───────────────────

export type CheatSheetBatterDetail = {
  batter_id: number;
  pitcher_id: number;
  game_pk: number;
  bat_side: string;
  bvp_career?: {
    pa?: number | null;
    hits?: number | null;
    hr?: number | null;
    avg?: number | null;
    iso?: number | null;
    slg?: number | null;
    obp?: number | null;
    k_pct?: number | null;
    bb_pct?: number | null;
  } | null;
  bvp_batted_ball?: {
    profile?: {
      barrel_pct?: number | null;
      hh_pct?: number | null;
      fb_pct?: number | null;
      gb_pct?: number | null;
      ld_pct?: number | null;
      pu_pct?: number | null;
      hr_fb_pct?: number | null;
      pull_pct?: number | null;
      str_pct?: number | null;
      oppo_pct?: number | null;
      total_batted?: number | null;
    } | null;
    log?: {
      date?: string | null;
      pitch?: string | null;
      ev?: number | null;
      angle?: number | null;
      dist?: number | null;
      trajectory?: string | null;
      result?: string | null;
      hr_parks?: number | null;
    }[];
  } | null;
  pitcher_splits?: Record<string, {
    ip?: number | null;
    home_runs?: number | null;
    hr_per_9?: number | null;
    barrel_pct?: number | null;
    hard_hit_pct?: number | null;
    fb_pct?: number | null;
    hr_fb_pct?: number | null;
    whip?: number | null;
    woba?: number | null;
  }> | null;
};

export function useMlbCheatSheetBatterDetail(
  batterId?: number | null,
  pitcherId?: number | null,
  gamePk?: number | null,
  batSide?: string | null,
) {
  const params = useMemo(
    () => ({
      batter_id: batterId ?? 0,
      pitcher_id: pitcherId ?? 0,
      game_pk: gamePk ?? 0,
      bat_side: batSide ?? "R",
    }),
    [batterId, pitcherId, gamePk, batSide],
  );
  return useEplQuery<CheatSheetBatterDetail>(
    "/mlb/matchups/cheat-sheet/batter-detail",
    params,
    Boolean(batterId && pitcherId && gamePk),
  );
}

/**
 * Fire-and-forget POST to pre-warm the server cache for all batter details.
 * Call once after the cheat sheet loads. Subsequent individual GET requests
 * will hit the warm server cache and return instantly.
 */
export function useCheatSheetBatchPreFetch(batters: CheatSheetBatter[] | null) {
  const hasFired = useRef(false);

  useEffect(() => {
    if (!batters || batters.length === 0 || hasFired.current) return;
    hasFired.current = true;

    const payload = batters
      .filter((b) => b.batter_id && b.pitcher_id && b.game_pk)
      .map((b) => ({
        batter_id: b.batter_id,
        pitcher_id: b.pitcher_id,
        game_pk: b.game_pk,
        bat_side: b.bat_side ?? "R",
      }));

    if (payload.length === 0) return;

    // Fire-and-forget POST to all fallback bases
    const bases = [API_BASE, CLOUD_API_BASE];
    if (typeof window !== "undefined" && window.location?.origin) {
      bases.push(`${window.location.origin}/api`);
    }
    const uniqueBases = Array.from(new Set(bases));

    for (const base of uniqueBases) {
      const url = `${base.replace(/\/+$/, "")}/mlb/matchups/cheat-sheet/batch-detail`;
      fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "omit",
        body: JSON.stringify({ batters: payload }),
      }).catch(() => {});
      break; // Only need to hit one base successfully
    }
  }, [batters]);
}

// ── NRFI / YRFI types ───────────────────────────────────────────────────

export type NrfiTeam = {
  team_id?: number | null;
  team_code?: string | null;
  team_name?: string | null;
  pitcher_id?: number | null;
  pitcher_name?: string | null;
  team_nrfi?: number;
  team_yrfi?: number;
  team_games_played?: number;
  team_nrfi_pct?: number | null;
  team_nrfi_record?: string | null;
  team_l10_nrfi?: number;
  team_l10_yrfi?: number;
  team_l10_record?: string | null;
  team_streak?: string | null;
  pitcher_nrfi?: number;
  pitcher_yrfi?: number;
  pitcher_games_started?: number;
  pitcher_nrfi_pct?: number | null;
  pitcher_nrfi_record?: string | null;
  pitcher_streak?: string | null;
};

export type NrfiMatchup = {
  game_id?: number | null;
  game_date?: string | null;
  nrfi_score?: number | null;
  home_team: NrfiTeam;
  away_team: NrfiTeam;
  // FanDuel deep link data
  fd_market_id?: string | null;
  nrfi_selection_id?: string | null;
  nrfi_odds?: number | null;
  yrfi_selection_id?: string | null;
  yrfi_odds?: number | null;
};

export type MlbNrfiData = {
  matchups: NrfiMatchup[];
  count: number;
  fd_state?: string;
};

export function useMlbNrfi(fdState: string = "nj") {
  const params = useMemo(() => ({ state: fdState }), [fdState]);
  return useEplQuery<MlbNrfiData>("/mlb/matchups/nrfi", params);
}

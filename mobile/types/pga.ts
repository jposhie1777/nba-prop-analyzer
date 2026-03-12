export type PgaPlayer = {
  id: number;
  first_name: string;
  last_name: string;
  display_name: string;
  country?: string | null;
  country_code?: string | null;
  owgr?: number | null;
  active?: boolean;
  player_image_url?: string | null;
};

export type PgaCourse = {
  id: number;
  name: string;
  city?: string | null;
  state?: string | null;
  country?: string | null;
  par?: number | null;
  yardage?: string | null;
  architect?: string | null;
  fairway_grass?: string | null;
  green_grass?: string | null;
};

export type PgaTournament = {
  id: number;
  season: number;
  name: string;
  start_date?: string | null;
  end_date?: string | null;
  city?: string | null;
  state?: string | null;
  country?: string | null;
  course_name?: string | null;
  status?: string | null;
  courses?: Array<{
    course: PgaCourse;
    rounds: number[];
  }>;
};

export type PgaRoundScore = {
  round_number?: number | null;
  round_date?: string | null;
  round_score?: number | null;
  par_relative_score?: number | null;
  total_score?: number | null;
};

export type PgaPlayerFormRow = {
  player_id: number;
  player: PgaPlayer;
  starts: number;
  avg_finish: number;
  top10_rate: number;
  top20_rate: number;
  cut_rate: number;
  consistency_index: number;
  form_score: number;
  recent_finishes: string[];
};

export type PgaPlacementRow = {
  player_id: number;
  player: PgaPlayer;
  starts: number;
  win_prob: number;
  top5_prob: number;
  top10_prob: number;
  top20_prob: number;
};

export type PgaCutRateRow = {
  player_id: number;
  player: PgaPlayer;
  starts: number;
  cuts: number;
  cut_rate: number;
  made_cut_rate: number;
};

export type PgaTournamentDifficultyRow = {
  tournament_id: number;
  tournament: PgaTournament;
  scoring_average: number | null;
  scoring_diff: number | null;
  difficulty_rank: number | null;
  birdie_rate: number;
  bogey_rate: number;
  par_rate: number;
};

export type PgaCourseComp = {
  course: PgaCourse;
  similarity: number;
};

export type PgaCourseFitRow = {
  player_id: number;
  player: PgaPlayer;
  course_events: number;
  comp_events: number;
  course_avg_finish: number | null;
  comp_avg_finish: number | null;
  course_fit_score: number;
};

export type PgaMatchupResponse = {
  player_id: number;
  opponent_id: number;
  starts: number;
  wins: number;
  losses: number;
  ties: number;
  win_rate: number;
  matches: Array<{
    tournament: PgaTournament;
    player_position?: string | null;
    opponent_position?: string | null;
    player_finish_value?: number | null;
    opponent_finish_value?: number | null;
  }>;
};

export type PgaRegionSplit = {
  key: string | number;
  starts: number;
  avg_finish: number | null;
  top10_rate: number;
};

export type PgaRegionSplitsResponse = {
  player_id: number;
  by_month: PgaRegionSplit[];
  by_country: PgaRegionSplit[];
};

export type PgaCourseProfile = {
  course: PgaCourse | null;
  summary: Record<string, number | null>;
  holes: Array<{
    hole_number: number;
    par: number;
    yardage?: number | null;
  }>;
};

export type PgaSimulatedFinishes = {
  player_id: number;
  simulations: number;
  starts?: number;
  distribution: Record<string, number>;
  top5_prob: number;
  top10_prob: number;
  top20_prob: number;
};

export type PgaSimulatedLeaderboardRow = {
  player_id: number;
  player: PgaPlayer;
  starts: number;
  projected_finish: number;
  projected_score: number;
  win_prob: number;
  top5_prob: number;
  top10_prob: number;
  top20_prob: number;
};

export type PgaSimulatedLeaderboard = {
  simulations: number;
  field_size: number;
  leaderboard: PgaSimulatedLeaderboardRow[];
};

export type PgaComparePlayer = {
  player_id: number;
  player: PgaPlayer;
  rank: number;
  score: number;
  metrics: {
    form_score?: number | null;
    course_fit_score?: number | null;
    head_to_head_win_rate?: number | null;
    head_to_head_starts?: number | null;
    top5_prob?: number | null;
    top10_prob?: number | null;
    top20_prob?: number | null;
    tournament_bonus?: number | null;
    tournament_avg_finish?: number | null;
    tournament_starts?: number | null;
    round_scores?: PgaRoundScore[];
  };
};

export type PgaCompareHeadToHead = {
  player_id: number;
  opponent_id: number;
  starts: number;
  wins: number;
  losses: number;
  ties: number;
  win_rate?: number | null;
};

export type PgaCompareRecommendation = {
  player_id: number;
  label: string;
  edge: number;
  reasons: string[];
};

export type PgaCompareResponse = {
  player_ids: number[];
  course_id?: number | null;
  tournament_id?: number | null;
  weights: Record<string, number>;
  players: PgaComparePlayer[];
  head_to_head: PgaCompareHeadToHead[];
  recommendation?: PgaCompareRecommendation | null;
  round_scores_tournament?: PgaTournament | null;
};

export type PgaBettingOutrightRow = {
  tournament_id?: string | null;
  player_id?: number | null;
  player_display_name?: string | null;
  american_odds?: number | null;
  implied_probability?: number | null;
  expected_round_score?: number | null;
  sg_total?: number | null;
  sg_approach?: number | null;
  sg_putting?: number | null;
  course_delta?: number | null;
};

export type PgaBettingFinishRow = {
  tournament_id?: string | null;
  tournament_name?: string | null;
  player_id?: string | null;
  player_display_name?: string | null;
  sub_market_name?: string | null;
  american_odds?: number | null;
  implied_probability?: number | null;
  model_probability?: number | null;
  betting_edge?: number | null;
  tournaments_played?: number | null;
  season_total_score_avg?: number | null;
  l5_total_score_avg?: number | null;
  cut_rate_l5?: number | null;
  top10_rate_l5?: number | null;
  sg_total?: number | null;
  sg_approach?: number | null;
  sg_putting?: number | null;
  course_delta?: number | null;
};

export type PgaBettingMatchupRow = {
  tournament_id?: string | null;
  sub_market_name?: string | null;
  group_index?: number | null;
  player_a?: string | null;
  player_b?: string | null;
  odds_a?: number | null;
  odds_b?: number | null;
  round_avg_a?: number | null;
  round_avg_b?: number | null;
  round_avg_diff?: number | null;
  cut_rate_diff?: number | null;
  top10_rate_diff?: number | null;
  sg_total_diff?: number | null;
  sg_approach_diff?: number | null;
  sg_putting_diff?: number | null;
};

export type PgaBetting3BallRow = {
  tournament_id?: string | null;
  group_index?: number | null;
  player_id?: string | null;
  player_display_name?: string | null;
  american_odds?: number | null;
  implied_probability?: number | null;
  round_avg?: number | null;
  cut_rate_l5?: number | null;
  top10_rate_l5?: number | null;
  sg_total?: number | null;
  sg_approach?: number | null;
  sg_putting?: number | null;
};

export type PgaPlayerSkillStatsRow = {
  player_id?: number | null;
  player_name?: string | null;
  sg_total?: number | null;
  sg_off_tee?: number | null;
  sg_approach?: number | null;
  sg_putting?: number | null;
  driving_distance?: number | null;
  driving_accuracy?: number | null;
  gir_pct?: number | null;
  scrambling_pct?: number | null;
  putting_avg?: number | null;
  putts_per_round?: number | null;
  scoring_avg?: number | null;
  birdie_avg?: number | null;
};

export type PgaRecentFormRow = {
  season?: number | null;
  player_id?: number | null;
  player_display_name?: string | null;
  tournaments_played?: number | null;
  season_finish_avg?: number | null;
  season_to_par_avg?: number | null;
  season_cuts_made?: number | null;
  l5_total_score_avg?: number | null;
  l5_finish_avg?: number | null;
  l5_to_par_avg?: number | null;
  l5_cuts_made?: number | null;
  l5_tournaments_considered?: number | null;
  cut_rate_l5?: number | null;
  top10_rate_l5?: number | null;
  weighted_l5_score?: number | null;
  form_trend_3?: number | null;
  days_since_last_event?: number | null;
  last_event_date?: string | null;
  season_r1_avg?: number | null;
  season_r2_avg?: number | null;
  season_r3_avg?: number | null;
  season_r4_avg?: number | null;
};

export type PgaPlayer = {
  id: number;
  first_name: string;
  last_name: string;
  display_name: string;
  country?: string | null;
  country_code?: string | null;
  owgr?: number | null;
  active?: boolean;
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
  distribution: Record<string, number>;
  top5_prob: number;
  top10_prob: number;
  top20_prob: number;
};

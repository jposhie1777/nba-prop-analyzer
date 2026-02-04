export type AtpPlayer = {
  id: number;
  first_name?: string | null;
  last_name?: string | null;
  full_name?: string | null;
  country?: string | null;
  country_code?: string | null;
  birth_place?: string | null;
  age?: number | null;
  height_cm?: number | null;
  weight_kg?: number | null;
  plays?: string | null;
  turned_pro?: number | null;
};

export type AtpTournament = {
  id: number;
  name?: string | null;
  location?: string | null;
  surface?: string | null;
  category?: string | null;
  season?: number | null;
  start_date?: string | null;
  end_date?: string | null;
};

export type AtpPlayerFormRow = {
  player_id: number;
  player: AtpPlayer;
  matches: number;
  wins: number;
  win_rate: number;
  straight_sets_rate: number;
  avg_sets: number | null;
  tiebreak_rate: number;
  form_score: number;
  recent_results: string[];
};

export type AtpSurfaceSplitRow = {
  surface: string;
  matches: number;
  wins: number;
  losses: number;
  win_rate: number;
  straight_sets_rate: number;
  avg_sets: number | null;
  tiebreak_rate: number;
};

export type AtpHeadToHeadResponse = {
  player_id: number;
  opponent_id: number;
  starts: number;
  wins: number;
  losses: number;
  win_rate: number;
  by_surface: Array<{
    surface: string;
    matches: number;
    wins: number;
    losses: number;
    win_rate: number;
  }>;
  matches: Array<{
    tournament?: AtpTournament | null;
    round?: string | null;
    surface?: string | null;
    result?: string | null;
    score?: string | null;
    start_date?: string | null;
  }>;
  seasons?: number[];
};

export type AtpTournamentPerformanceRow = {
  player_id: number;
  player: AtpPlayer;
  tournaments: number;
  titles: number;
  finals: number;
  semis: number;
  quarters: number;
  match_wins: number;
  match_losses: number;
  win_rate: number;
};

export type AtpRegionSplitRow = {
  key: string | number;
  matches: number;
  wins: number;
  win_rate: number;
};

export type AtpRegionSplitsResponse = {
  player_id: number;
  by_month: AtpRegionSplitRow[];
  by_location: AtpRegionSplitRow[];
  seasons?: number[];
};

export type AtpSetDistribution = {
  player_id: number;
  surface?: string | null;
  wins: Record<string, number>;
  losses: Record<string, number>;
  win_rates: Record<string, number>;
  loss_rates: Record<string, number>;
  seasons?: number[];
};

export type AtpComparePlayer = {
  player_id: number;
  rank: number;
  score: number;
  metrics: {
    form_score: number;
    recent_win_rate: number;
    surface_win_rate?: number | null;
    ranking?: number | null;
  };
};

export type AtpCompareRecommendation = {
  player_id: number;
  label: string;
  edge: number;
  reasons: string[];
};

export type AtpCompareResponse = {
  player_ids: number[];
  surface?: string | null;
  weights: Record<string, number>;
  players: AtpComparePlayer[];
  head_to_head?: AtpHeadToHeadResponse | null;
  recommendation?: AtpCompareRecommendation | null;
};

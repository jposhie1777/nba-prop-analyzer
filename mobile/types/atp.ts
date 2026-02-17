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
    recent_form_win_rate?: number | null;
    recent_surface_win_rate?: number | null;
    straight_sets_win_rate?: number | null;
    tiebreak_rate?: number | null;
    win_rate_vs_top50?: number | null;
    l15_win_rate?: number | null;
    l40_win_rate?: number | null;
    current_win_streak?: number | null;
    current_loss_streak?: number | null;
    sample_confidence?: number | null;
    titles?: number | null;
  };
};

export type AtpBettingAnalyticsPlayer = {
  player_id: number;
  player_name?: string | null;
  surface_key: string;
  world_rank?: number | null;
  ranking_points?: number | null;
  total_matches?: number | null;
  total_wins?: number | null;
  raw_win_rate?: number | null;
  adj_win_rate?: number | null;
  l10_matches?: number | null;
  l10_wins?: number | null;
  l10_win_rate?: number | null;
  l10_adj_win_rate?: number | null;
  l15_matches?: number | null;
  l15_wins?: number | null;
  l15_win_rate?: number | null;
  l15_adj_win_rate?: number | null;
  l20_matches?: number | null;
  l20_wins?: number | null;
  l20_win_rate?: number | null;
  l40_matches?: number | null;
  l40_wins?: number | null;
  l40_win_rate?: number | null;
  l40_adj_win_rate?: number | null;
  l10_surface_matches?: number | null;
  l10_surface_wins?: number | null;
  l10_surface_win_rate?: number | null;
  l10_surface_adj_win_rate?: number | null;
  l20_surface_matches?: number | null;
  l20_surface_wins?: number | null;
  l20_surface_win_rate?: number | null;
  l20_surface_adj_win_rate?: number | null;
  matches_vs_top50?: number | null;
  wins_vs_top50?: number | null;
  win_rate_vs_top50?: number | null;
  adj_win_rate_vs_top50?: number | null;
  straight_sets_wins?: number | null;
  straight_sets_rate?: number | null;
  tiebreak_matches?: number | null;
  tiebreak_rate?: number | null;
  avg_sets_per_match?: number | null;
  retirement_matches?: number | null;
  retirement_rate?: number | null;
  tournaments_played?: number | null;
  titles?: number | null;
  finals_reached?: number | null;
  semis_reached?: number | null;
  quarters_reached?: number | null;
  grand_slam_matches?: number | null;
  grand_slam_wins?: number | null;
  grand_slam_win_rate?: number | null;
  masters_matches?: number | null;
  masters_wins?: number | null;
  masters_win_rate?: number | null;
  current_win_streak?: number | null;
  current_loss_streak?: number | null;
  betting_form_score?: number | null;
  sample_confidence?: number | null;
  updated_at?: string | null;
};

export type AtpBettingAnalyticsResponse = {
  surface?: string | null;
  count: number;
  players: Record<number, AtpBettingAnalyticsPlayer>;
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

// types/teamStats.ts
export type TeamSeasonStats = {
  team_id: number;
  team_abbr: string;
  team_name: string;
  gp: number;
  pts: number;
  pts_rank: number;
  fg_pct: number;
  fg_pct_rank: number;
  fg3_pct: number;
  fg3_pct_rank: number;
  ft_pct: number;
  ft_pct_rank: number;
  ast: number;
  ast_rank: number;
  reb: number;
  reb_rank: number;
  tov: number;
  tov_rank: number;
};
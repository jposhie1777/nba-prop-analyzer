import { useQuery } from "@tanstack/react-query";

const API = process.env.EXPO_PUBLIC_API_URL!;

export type FirstBasketRow = {
  game_id: number;
  game_date: string;

  team_id: number;
  team_abbr: string;

  player_id: number;
  player: string;

  starter_pct: number;
  rotation_tier: string;
  first_shot_share: number;
  team_first_score_rate: number;

  pts_per_min: number;
  fga_per_min: number;
  usage_l10: number;

  player_first_basket_count: number;
  player_team_first_basket_count: number;
  team_first_basket_count: number;

  raw_projection_score: number;
  first_basket_probability: number;

  rank_within_team: number;
  rank_within_game: number;

  team_tip_win_pct: number;

  projected_at: string;
  model_version: string;
};

export function useFirstBasket() {
  return useQuery<FirstBasketRow[]>({
    queryKey: ["first-basket"],
    queryFn: async () => {
      const res = await fetch(`${API}/first-basket`);
      if (!res.ok) throw new Error("Failed to fetch first basket");
      return res.json();
    },
    staleTime: 60_000,
  });
}
import { API_BASE } from "./config";

export type ThreeQ100Team = {
  team_abbr: string;
  opponent_abbr: string;
  side: "HOME" | "AWAY";
  games_played?: number | null;
  games_defended?: number | null;
  avg_3q_points?: number | null;
  avg_3q_allowed?: number | null;
  hit_100_rate?: number | null;
  allow_100_rate?: number | null;
  predicted_hit_rate?: number | null;
  predicted_3q_points?: number | null;
};

export type ThreeQ100Game = {
  game_id: number;
  game_date: string;
  start_time_est?: string | null;
  home_team_abbr: string;
  away_team_abbr: string;
  teams: ThreeQ100Team[];
};

export type ThreeQ100Response = {
  game_date: string;
  generated_at?: string | null;
  count: number;
  games: ThreeQ100Game[];
};

export async function fetchThreeQ100(params?: {
  gameDate?: string;
  refresh?: boolean;
}): Promise<ThreeQ100Response> {
  const qs = new URLSearchParams();

  if (params?.gameDate) {
    qs.append("game_date", params.gameDate);
  }

  if (params?.refresh) {
    qs.append("refresh", "true");
  }

  const query = qs.toString();
  const url = `${API_BASE}/analytics/three-q-100${query ? `?${query}` : ""}`;

  const res = await fetch(url, {
    method: "GET",
    credentials: "omit",
    headers: {
      "Content-Type": "application/json",
    },
  });

  const text = await res.text();

  if (!res.ok) {
    throw new Error(`API ${res.status}: ${text}`);
  }

  try {
    return JSON.parse(text) as ThreeQ100Response;
  } catch (err) {
    console.error("Invalid JSON from 3Q-100 endpoint", err);
    throw new Error("Invalid JSON response from API");
  }
}

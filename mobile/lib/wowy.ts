// lib/wowy.ts
import { API_BASE } from "./config";

// ==================================================
// Types
// ==================================================
export type TeammateWowy = {
  player_id: number;
  teammate_name: string;
  games_with: number;
  games_without: number;
  // Stats WITH the injured player
  pts_with: number | null;
  reb_with: number | null;
  ast_with: number | null;
  min_with: number | null;
  fg3m_with: number | null;
  plus_minus_with: number | null;
  // Stats WITHOUT the injured player
  pts_without: number | null;
  reb_without: number | null;
  ast_without: number | null;
  min_without: number | null;
  fg3m_without: number | null;
  plus_minus_without: number | null;
  // Differences (positive = better without)
  pts_diff: number | null;
  reb_diff: number | null;
  ast_diff: number | null;
  min_diff: number | null;
  fg3m_diff: number | null;
  plus_minus_diff: number | null;
};

export type TeamImpact = {
  games_with: number;
  games_without: number;
  team_ppg_with: number | null;
  team_ppg_without: number | null;
  team_ppg_diff: number | null;
};

export type InjuredPlayerInfo = {
  player_id: number;
  player_name: string;
  team: string | null;   // ✅ fix
  status: string;
  injury_type: string | null;
};


export type InjuredPlayerWowy = {
  injured_player: InjuredPlayerInfo;
  team_impact: TeamImpact;
  teammates: TeammateWowy[];
  teammate_count: number;
};

export type WowyResponse = {
  count: number;
  season: number;
  injured_players: InjuredPlayerWowy[];
};

export type SinglePlayerWowyResponse = {
  status: string;
  target_player: {
    player_id: number;
    player_name: string;
    team_id: number;
    team: string;
  };
  season: number;
  team_impact: TeamImpact;
  teammates: TeammateWowy[];
  teammate_count: number;
};

export type BeneficiariesResponse = {
  player_id: number;
  stat: string;
  beneficiaries: TeammateWowy[];
};

// ==================================================
// API Functions
// ==================================================
export async function fetchWowyForInjured(params?: {
  team?: string;
  status?: string;
  season?: number;
}): Promise<WowyResponse> {
  const qs = new URLSearchParams();

  if (params?.team) {
    qs.append("team", params.team);
  }
  if (params?.status) {
    qs.append("status", params.status);
  }
  if (params?.season) {
    qs.append("season", params.season.toString());
  }

  // 🔴 REQUIRED FIX — disable backend default filter
  qs.append("today_only", "false");

  const url = `${API_BASE}/injuries/wowy/injured?${qs.toString()}`;

  const res = await fetch(url, {
    method: "GET",
    credentials: "omit",
    headers: {
      "Content-Type": "application/json",
    },
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`WOWY API ${res.status}: ${text}`);
  }

  return res.json();
}


export async function fetchWowyForPlayer(
  playerId: number,
  params?: {
    season?: number;
    minGamesWith?: number;
    minGamesWithout?: number;
  }
): Promise<SinglePlayerWowyResponse> {
  const qs = new URLSearchParams();

  if (params?.season) {
    qs.append("season", params.season.toString());
  }
  if (params?.minGamesWith) {
    qs.append("min_games_with", params.minGamesWith.toString());
  }
  if (params?.minGamesWithout) {
    qs.append("min_games_without", params.minGamesWithout.toString());
  }

  const url = `${API_BASE}/injuries/wowy/player/${playerId}${qs.toString() ? `?${qs.toString()}` : ""}`;

  const res = await fetch(url, {
    method: "GET",
    credentials: "omit",
    headers: {
      "Content-Type": "application/json",
    },
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`WOWY Player API ${res.status}: ${text}`);
  }

  return res.json();
}

export async function fetchBeneficiaries(
  playerId: number,
  params?: {
    stat?: "pts" | "reb" | "ast" | "fg3m" | "min";
    limit?: number;
    season?: number;
  }
): Promise<BeneficiariesResponse> {
  const qs = new URLSearchParams();

  if (params?.stat) {
    qs.append("stat", params.stat);
  }
  if (params?.limit) {
    qs.append("limit", params.limit.toString());
  }
  if (params?.season) {
    qs.append("season", params.season.toString());
  }

  const url = `${API_BASE}/injuries/wowy/beneficiaries/${playerId}${qs.toString() ? `?${qs.toString()}` : ""}`;

  const res = await fetch(url, {
    method: "GET",
    credentials: "omit",
    headers: {
      "Content-Type": "application/json",
    },
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Beneficiaries API ${res.status}: ${text}`);
  }

  return res.json();
}

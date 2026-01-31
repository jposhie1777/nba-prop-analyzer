// lib/injuries.ts
import { API_BASE } from "./config";

// ==================================================
// Types
// ==================================================
export type InjuryRecord = {
  injury_id: number | null;
  player_id: number;
  player_name: string;
  player_first_name: string;
  player_last_name: string;
  team_id: number;
  team_abbreviation: string;
  team_name: string | null;
  status: string; // "Out", "Questionable", "Doubtful", "Day-To-Day", "Probable"
  injury_type: string | null;
  report_date: string | null;
  return_date: string | null;
  ingested_at: string;
};

export type TeamInjuries = {
  team: string;
  team_name: string | null;
  team_id: number;
  injuries: InjuryRecord[];
};

export type InjuriesResponse = {
  count: number;
  injuries: InjuryRecord[];
  by_team: TeamInjuries[];
  status_summary: {
    out: number;
    doubtful: number;
    questionable: number;
    day_to_day: number;
    probable: number;
  };
};

// ==================================================
// API Functions
// ==================================================
export async function fetchInjuries(params?: {
  team?: string;
  status?: string;
}): Promise<InjuriesResponse> {
  const qs = new URLSearchParams();

  if (params?.team) {
    qs.append("team", params.team);
  }
  if (params?.status) {
    qs.append("status", params.status);
  }

  const url = `${API_BASE}/injuries${qs.toString() ? `?${qs.toString()}` : ""}`;

  const res = await fetch(url, {
    method: "GET",
    credentials: "omit",
    headers: {
      "Content-Type": "application/json",
    },
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Injuries API ${res.status}: ${text}`);
  }

  return res.json();
}

export async function fetchTeamInjuries(
  teamAbbr: string
): Promise<InjuriesResponse> {
  const url = `${API_BASE}/injuries/team/${teamAbbr.toUpperCase()}`;

  const res = await fetch(url, {
    method: "GET",
    credentials: "omit",
    headers: {
      "Content-Type": "application/json",
    },
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Team injuries API ${res.status}: ${text}`);
  }

  return res.json();
}

// lib/sharpMoves.ts
import { API_BASE } from "@/lib/config";

// ─── Types ───────────────────────────────────────

export type SpreadMovement = {
  opening: number | null;
  current: number | null;
  shift: number | null;
  label: string;
  direction: string | null;
  opening_odds: { home: number | null; away: number | null };
  current_odds: { home: number | null; away: number | null };
};

export type TotalMovement = {
  opening: number | null;
  current: number | null;
  shift: number | null;
  label: string;
  direction: string | null;
  opening_odds: { over: number | null; under: number | null };
  current_odds: { over: number | null; under: number | null };
};

export type MoneylineMovement = {
  opening_home: number | null;
  current_home: number | null;
  opening_away: number | null;
  current_away: number | null;
};

export type BookMovement = {
  book: string;
  total_snapshots: number;
  opening_ts: string | null;
  current_ts: string | null;
  spread: SpreadMovement;
  total: TotalMovement;
  moneyline: MoneylineMovement;
};

export type GameSummary = {
  avg_spread_shift: number | null;
  avg_total_shift: number | null;
  max_spread_move: number;
  max_total_move: number;
  is_sharp: boolean;
  is_steam: boolean;
  alert_level: "steam" | "sharp" | "notable" | "quiet";
  insights: string[];
};

export type SharpMoveGame = {
  game_id: number;
  home_team_abbr: string | null;
  away_team_abbr: string | null;
  game_time_et: string | null;
  books: BookMovement[];
  summary: GameSummary | null;
};

export type SharpMovesResponse = {
  game_date: string;
  count: number;
  sharp_count: number;
  games: SharpMoveGame[];
  error?: string;
};

// ─── Fetch ───────────────────────────────────────

export async function fetchSharpMoves(params?: {
  gameDate?: string;
  book?: string;
}): Promise<SharpMovesResponse> {
  const search = new URLSearchParams();
  if (params?.gameDate) search.set("game_date", params.gameDate);
  if (params?.book) search.set("book", params.book);

  const qs = search.toString();
  const url = `${API_BASE}/analytics/sharp-moves${qs ? `?${qs}` : ""}`;

  const res = await fetch(url, { credentials: "omit" });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || `HTTP ${res.status}`);
  }
  return res.json();
}

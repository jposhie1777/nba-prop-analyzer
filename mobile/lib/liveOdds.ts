const API = process.env.EXPO_PUBLIC_API_URL!;

// ------------------------------
// Types
// ------------------------------

export type LivePlayerProp = {
  player_id: number;
  market: "PTS" | "AST" | "REB" | "3PM";
  line: number;
  book: "draftkings" | "fanduel";
  over: number;
  under: number;
};

export type LiveGameOdds = {
  book: "draftkings" | "fanduel";
  spread: number | null;
  spread_odds: number | null;
  total: number | null;
  over: number | null;
  under: number | null;
};

// ------------------------------
// API calls
// ------------------------------

export async function fetchLivePlayerProps(gameId: number) {
  const res = await fetch(
    `${API}/live/odds/player-props?game_id=${gameId}`
  );

  if (!res.ok) {
    throw new Error("Failed to fetch live player props");
  }

  return res.json() as Promise<{
    game_id: number;
    updated_at: string | null;
    props: LivePlayerProp[];
  }>;
}

export async function fetchLiveGameOdds(gameId: number) {
  const res = await fetch(
    `${API}/live/odds/games?game_id=${gameId}`
  );

  if (!res.ok) {
    throw new Error("Failed to fetch live game odds");
  }

  return res.json() as Promise<{
    game_id: number;
    updated_at: string | null;
    odds: LiveGameOdds[];
  }>;
}
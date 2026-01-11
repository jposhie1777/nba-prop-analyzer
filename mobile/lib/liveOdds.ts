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

  const json = await res.json();

  if (__DEV__) {
    console.log("🌐 fetchLivePlayerProps()", {
      gameId,
      count: json.props?.length,
      sample: json.props?.[0],
    });
  }

  return json as {
    game_id: number;
    updated_at: string | null;
    props: LivePlayerProp[];
  };
}
export async function fetchLiveGameOdds(gameId: number) {
  const res = await fetch(
    `${API}/live/odds/games?game_id=${gameId}`
  );

  if (!res.ok) {
    throw new Error("Failed to fetch live game odds");
  }

  const json = await res.json();

  if (__DEV__) {
    console.log("🌐 fetchLiveGameOdds()", {
      gameId,
      count: json.odds?.length,
      sample: json.odds?.[0],
    });
  }

  return json as {
    game_id: number;
    updated_at: string | null;
    odds: LiveGameOdds[];
  };
}
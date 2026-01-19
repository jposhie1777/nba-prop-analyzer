// services/liveAnalytics.ts
import Constants from "expo-constants";

const API = Constants.expoConfig?.extra?.API_URL!;

export async function fetchLivePropAnalytics(params: {
  gameId: number;
  playerId: number;
  market: string;
  line: number;
  side: "over" | "under" | "milestone";
}) {
  try {
    const qs = new URLSearchParams({
      game_id: String(params.gameId),
      player_id: String(params.playerId),
      market: params.market.toLowerCase(),
      line: String(params.line),
      side: params.side,
    });

    const res = await fetch(`${API}/live/prop-analytics?${qs}`);
    if (!res.ok) return null;

    return await res.json();
  } catch {
    return null;
  }
}

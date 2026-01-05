import { adaptLiveGames } from "./adapters/liveAdapter";
import { LiveGame } from "@/types/live";

const API_URL = process.env.EXPO_PUBLIC_LIVE_API;

export function subscribeLiveGames(
  onUpdate: (games: LiveGame[]) => void
) {
  if (!API_URL) return () => {};

  const es = new EventSource(`${API_URL}/live/games/stream`);

  es.onmessage = (e) => {
    try {
      const raw = JSON.parse(e.data);
      onUpdate(adaptLiveGames(raw.games ?? raw));
    } catch {}
  };

  es.onerror = () => {
    es.close();
  };

  return () => es.close();
}

// services/liveGame
import Constants from "expo-constants";
import { adaptLiveGames } from "./adapters/liveAdapter";
import { LiveGame } from "@/types/live";

const API_URL = Constants.expoConfig?.extra?.API_URL as string | undefined;

export function subscribeLiveGames(
  onUpdate: (games: LiveGame[]) => void
) {
  if (!API_URL) {
    console.warn("❌ API_URL is undefined — live games disabled");
    return () => {};
  }

  const url = `${API_URL}/live/scores/stream`;
  console.log("📡 Connecting to live SSE:", url);

  const es = new EventSource(url);

  es.addEventListener("snapshot", (e) => {
    try {
      const raw = JSON.parse(e.data);
      onUpdate(adaptLiveGames(raw.games ?? []));
    } catch (err) {
      console.error("❌ Live snapshot parse failed", err);
    }
  });

  es.onerror = (err) => {
    console.error("❌ Live SSE error", err);
    es.close();
  };

  return () => es.close();
}
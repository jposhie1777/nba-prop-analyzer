import { useEffect, useState } from "react";
import Constants from "expo-constants";

const API = Constants.expoConfig?.extra?.API_URL!;
const POLL_MS = 30_000;

export type LiveGameSchedule = {
  game_id: number;
  home: string;
  away: string;
  start_time_et: string | null;
  state: "UPCOMING" | "LIVE";
};

export function useLiveGameSchedule() {
  const [games, setGames] = useState<LiveGameSchedule[]>([]);

  useEffect(() => {
    let timer: ReturnType<typeof setInterval>;

    const poll = async () => {
      try {
        const res = await fetch(`${API}/live/games`);
        const json = await res.json();
        setGames(json.games ?? []);
      } catch (e) {
        console.warn("[LiveGameSchedule] poll failed", e);
      }
    };

    poll();
    timer = setInterval(poll, POLL_MS);

    return () => clearInterval(timer);
  }, []);

  return { games };
}

import { useEffect, useRef, useState } from "react";
import { AppState } from "react-native";
import { LiveGame } from "@/types/live";
import { adaptLiveGames } from "@/services/adapters/liveAdapters";

const API = process.env.EXPO_PUBLIC_LIVE_API!;
const POLL_INTERVAL_MS = 20_000;

type Mode = "sse" | "poll";

export function useLiveGames() {
  const [games, setGames] = useState<LiveGame[]>([]);
  const [mode, setMode] = useState<Mode>("sse");

  const esRef = useRef<EventSource | null>(null);
  const pollRef = useRef<NodeJS.Timeout | null>(null);
  const appStateRef = useRef(AppState.currentState);

  /* =============================
     Polling
  ============================== */

  const startPolling = () => {
    if (pollRef.current) return;

    const poll = async () => {
      try {
        const res = await fetch(`${API}/live/scores`);
        if (!res.ok) throw new Error("poll failed");

        const json = await res.json();
        setGames(adaptLiveGames(json.games ?? []));
      } catch (e) {
        console.warn("Live polling error", e);
      }
    };

    poll(); // immediate
    pollRef.current = setInterval(poll, POLL_INTERVAL_MS);
    setMode("poll");
  };

  const stopPolling = () => {
    if (pollRef.current) {
      clearInterval(pollRef.current);
      pollRef.current = null;
    }
  };

  /* =============================
     SSE
  ============================== */

  const startSSE = () => {
    if (esRef.current) return;

    try {
      const es = new EventSource(`${API}/live/scores/stream`);
      esRef.current = es;

      es.addEventListener("snapshot", (e: MessageEvent) => {
        try {
          const raw = JSON.parse(e.data);
          setGames(adaptLiveGames(raw.games ?? []));
          setMode("sse");
        } catch {}
      });

      es.onerror = () => {
        es.close();
        esRef.current = null;

        // fallback
        startPolling();
      };
    } catch {
      startPolling();
    }
  };

  const stopSSE = () => {
    if (esRef.current) {
      esRef.current.close();
      esRef.current = null;
    }
  };

  /* =============================
     App lifecycle
  ============================== */

  useEffect(() => {
    startSSE();

    const sub = AppState.addEventListener("change", (nextState) => {
      const prev = appStateRef.current;
      appStateRef.current = nextState;

      // app resumed
      if (prev.match(/inactive|background/) && nextState === "active") {
        stopPolling();
        stopSSE();
        startSSE();
      }

      // app backgrounded
      if (nextState === "background") {
        stopSSE();
        startPolling();
      }
    });

    return () => {
      stopSSE();
      stopPolling();
      sub.remove();
    };
  }, []);

  return {
    games,
    mode, // "sse" | "poll"
    isLive: games.length > 0,
  };
}

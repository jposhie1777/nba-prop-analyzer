import { useEffect, useRef, useState } from "react";
import { AppState } from "react-native";
import { LiveGame } from "@/types/live";
import { adaptLiveGames } from "@/services/adapters/liveAdapter";
import Constants from "expo-constants";

/* ======================================================
   Config
====================================================== */

const API = Constants.expoConfig?.extra?.API_URL!;
const POLL_INTERVAL_MS = 20_000;

type Mode = "sse" | "poll";

/* ======================================================
   Hook
====================================================== */

export function useLiveGames() {
  const [games, setGames] = useState<LiveGame[]>([]);
  const [mode, setMode] = useState<Mode>("sse");

  const esRef = useRef<EventSource | null>(null);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const appStateRef = useRef(AppState.currentState);

  /* ======================================================
     DEBUG — one-time API resolution
  ====================================================== */

  useEffect(() => {
    console.log("🧠 [LiveGames] API resolved to:", API);
  }, []);

  /* ======================================================
     Polling
  ====================================================== */

  const startPolling = () => {
    if (pollRef.current) {
      console.log("🔁 [LiveGames] Polling already active — skipping");
      return;
    }

    console.log("🔁 [LiveGames] Starting POLLING mode");

    const poll = async () => {
      try {
        console.log("🔁 [LiveGames] Poll request → /live/scores");

        const res = await fetch(`${API}/live/scores`);
        if (!res.ok) {
          throw new Error(`Poll failed: ${res.status}`);
        }

        const json = await res.json();
        const adapted = adaptLiveGames(json.games ?? []);

        console.log(
          "📥 [LiveGames] Poll response:",
          adapted.length,
          "games"
        );

        setGames(adapted);
        setMode("poll");
      } catch (e) {
        console.warn("⚠️ [LiveGames] Polling error", e);
      }
    };

    poll(); // immediate first poll
    pollRef.current = setInterval(poll, POLL_INTERVAL_MS);
  };

  const stopPolling = () => {
    if (pollRef.current) {
      console.log("⛔ [LiveGames] Stopping polling");
      clearInterval(pollRef.current);
      pollRef.current = null;
    }
  };

  /* ======================================================
     SSE
  ====================================================== */

  const startSSE = () => {
    if (esRef.current) {
      console.log("📡 [LiveGames] SSE already active — skipping");
      return;
    }

    console.log("📡 [LiveGames] Starting SSE → /live/scores/stream");

    try {
      const es = new EventSource(`${API}/live/scores/stream`);
      esRef.current = es;

      es.addEventListener("open", () => {
        console.log("🟢 [LiveGames] SSE connection opened");
      });

      es.addEventListener("snapshot", (e: MessageEvent) => {
        try {
          const raw = JSON.parse(e.data);
          const adapted = adaptLiveGames(raw.games ?? []);

          console.log(
            "📥 [LiveGames] SSE snapshot:",
            adapted.length,
            "games"
          );

          setGames(adapted);
          setMode("sse");
        } catch (err) {
          console.error("❌ [LiveGames] SSE snapshot parse error", err);
        }
      });

      es.onerror = (err) => {
        console.error(
          "🔴 [LiveGames] SSE error — falling back to polling",
          err
        );

        es.close();
        esRef.current = null;
        startPolling();
      };
    } catch (err) {
      console.error("❌ [LiveGames] Failed to start SSE", err);
      startPolling();
    }
  };

  const stopSSE = () => {
    if (esRef.current) {
      console.log("⛔ [LiveGames] Closing SSE connection");
      esRef.current.close();
      esRef.current = null;
    }
  };

  /* ======================================================
     App lifecycle
  ====================================================== */

  useEffect(() => {
    console.log("🚀 [LiveGames] Hook mounted — initializing SSE");
    startSSE();

    const sub = AppState.addEventListener("change", (nextState) => {
      const prev = appStateRef.current;
      appStateRef.current = nextState;

      console.log(
        "🔄 [LiveGames] AppState change:",
        prev,
        "→",
        nextState
      );

      // App resumed
      if (prev.match(/inactive|background/) && nextState === "active") {
        console.log("▶️ [LiveGames] App resumed — restarting SSE");
        stopPolling();
        stopSSE();
        startSSE();
      }

      // App backgrounded
      if (nextState === "background") {
        console.log("⏸️ [LiveGames] App backgrounded — switching to polling");
        stopSSE();
        startPolling();
      }
    });

    return () => {
      console.log("🧹 [LiveGames] Hook unmount — cleaning up");
      stopSSE();
      stopPolling();
      sub.remove();
    };
  }, []);

  /* ======================================================
     Public API
  ====================================================== */

  return {
    games,
    mode,          // "sse" | "poll"
    isLive: games.length > 0,
  };
}
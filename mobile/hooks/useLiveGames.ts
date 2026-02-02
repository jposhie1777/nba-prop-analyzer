// hooks/useLiveGames.ts
import { useEffect, useRef, useState } from "react";
import { AppState } from "react-native";
import { LiveGame } from "@/types/live";
import { adaptLiveGames } from "@/services/adapters/liveAdapter";
import Constants from "expo-constants";
import { useDevStore } from "@/lib/dev/devStore";

/* ======================================================
   Config
====================================================== */

const API = Constants.expoConfig?.extra?.API_URL!;
const POLL_INTERVAL_MS = 60_000;

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

        setGames((prev) => {
          if (prev.length > 0 && adapted.length === 0) {
            return prev;
          }
          return adapted;
        });
        
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
    // 🚫 EventSource does NOT exist in React Native (iOS/Android)
    if (typeof EventSource === "undefined") {
      console.log(
        "📡 [LiveGames] SSE not supported on native — falling back to polling"
      );
  
      /* 🔴 DEV */
      useDevStore
        .getState()
        .actions.reportSSEDisconnect("SSE unsupported on native");
  
      startPolling();
      return;
    }
  
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
  
        /* 🔴 DEV */
        useDevStore.getState().actions.reportSSEConnect();
      });
  
      es.addEventListener("snapshot", (e: MessageEvent) => {
        /* 🔴 DEV */
        useDevStore.getState().actions.reportSSEEvent();
  
        try {
          const raw = JSON.parse(e.data);
          const adapted = adaptLiveGames(raw.games ?? []);
  
          setGames((prev) => {
            if (prev.length > 0 && adapted.length === 0) {
              return prev;
            }
            return adapted;
          });
  
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
  
        /* 🔴 DEV */
        useDevStore
          .getState()
          .actions.reportSSEDisconnect("LiveGames SSE error");
  
        es.close();
        esRef.current = null;
        startPolling();
      };
    } catch (err) {
      console.error("❌ [LiveGames] Failed to start SSE", err);
  
      /* 🔴 DEV */
      useDevStore
        .getState()
        .actions.reportSSEDisconnect("SSE init failed");
  
      startPolling();
    }
  };

  const stopSSE = () => {
    if (esRef.current) {
      console.log("⛔ [LiveGames] Closing SSE connection");

      /* 🔴 ADD */
      useDevStore
        .getState()
        .actions.reportSSEDisconnect("SSE closed");

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

        /* 🔴 ADD */
        useDevStore
          .getState()
          .actions.reportSSEDisconnect("App resumed — restarting SSE");

        stopPolling();
        stopSSE();
        startSSE();
      }

      // App backgrounded
      if (nextState === "background") {
        console.log("⏸️ [LiveGames] App backgrounded — switching to polling");

        /* 🔴 ADD */
        useDevStore
          .getState()
          .actions.reportSSEDisconnect("App backgrounded");

        stopSSE();
        startPolling();
      }
    });

    return () => {
      console.log("🧹 [LiveGames] Hook unmount — cleaning up");

      /* 🔴 ADD */
      useDevStore
        .getState()
        .actions.reportSSEDisconnect("Hook unmounted");

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
// hooks/useLivePlayerStats.ts
import { useEffect, useRef, useState } from "react";
import { AppState } from "react-native";
import Constants from "expo-constants";
import { useParlayTracker } from "@/store/useParlayTracker";

const API = Constants.expoConfig?.extra?.API_URL!;
const POLL_INTERVAL_MS = 60_000;

export type LivePlayerStat = {
  game_id: number;
  player_id: number;
  name: string;
  team: string;
  opponent: string;
  minutes: string | null;
  pts: number;
  reb: number;
  ast: number;
  fg3m: number;
  stl: number;
  blk: number;
  tov: number;
  fg: [number, number];
  fg3: [number, number];
  ft: [number, number];
  plus_minus: number;
  period: number | null;
  clock: string | null;
};

type Snapshot = {
  players: LivePlayerStat[];
  meta: {
    status: "OK" | "DEGRADED" | "BOOTING";
    server_updated_at?: string;
    source_updated_at?: string;
  };
};

type Mode = "sse" | "poll";

export function useLivePlayerStats() {
  const [players, setPlayers] = useState<LivePlayerStat[]>([]);
  const [mode, setMode] = useState<Mode>("sse");

  const esRef = useRef<EventSource | null>(null);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const appStateRef = useRef(AppState.currentState);

  const applyLiveSnapshot = useParlayTracker(
    (s) => s.applyLiveSnapshot
  );
  /* ===========================
     Polling
  =========================== */

  const startPolling = () => {
    if (pollRef.current) return;

    const poll = async () => {
      try {
        const res = await fetch(`${API}/live/player-stats`);
        if (!res.ok) throw new Error("poll failed");

        const json: Snapshot = await res.json();
        setPlayers(json.players ?? []);
        setMode("poll");
      } catch (e) {
        console.warn("⚠️ PlayerStats poll failed", e);
      }
    };

    poll();
    pollRef.current = setInterval(poll, POLL_INTERVAL_MS);
  };

  const stopPolling = () => {
    if (pollRef.current) {
      clearInterval(pollRef.current);
      pollRef.current = null;
    }
  };

  /* ===========================
     SSE
  =========================== */

  const startSSE = () => {
    if (esRef.current) return;

    try {
      const es = new EventSource(`${API}/live/player-stats/stream`);
      esRef.current = es;

      es.addEventListener("snapshot", (e: MessageEvent) => {
        try {
          const json: Snapshot = JSON.parse(e.data);
          setPlayers(
            (json.players ?? []).map((p) => ({
              ...p,
              fg3m: p.fg3?.[0] ?? 0, // 🔥 ADD THIS
            }))
          );
          setMode("sse");
        } catch (err) {
          console.error("❌ PlayerStats SSE parse error", err);
        }
      });

      es.onerror = () => {
        es.close();
        esRef.current = null;
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

  /* ===========================
     App lifecycle
  =========================== */

  useEffect(() => {
    startSSE();

    const sub = AppState.addEventListener("change", (next) => {
      const prev = appStateRef.current;
      appStateRef.current = next;

      if (prev.match(/inactive|background/) && next === "active") {
        stopPolling();
        stopSSE();
        startSSE();
      }

      if (next === "background") {
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

    /* ===========================
     Helper selector
  =========================== */

  const playersByGame = (gameId: number | string) => {
    const filtered = players.filter(
      (p) => String(p.game_id) === String(gameId)
    );

    console.log("🟡 DEBUG playersByGame", {
      gameId,
      totalPlayers: players.length,
      matchedPlayers: filtered.length,
      samplePlayerGameId: players[0]?.game_id,
    });

    return filtered;
  };

    /* ===========================
       Sync live stats → tracked parlays
    =========================== */
    useEffect(() => {
      if (!players.length) return;
  
      const snapshotByPlayerId = Object.fromEntries(
        players.map((p) => [
          String(p.player_id), // 🔑 STRING KEY (web-safe)
          {
            pts: p.pts,
            reb: p.reb,
            ast: p.ast,
            fg3m: p.fg3m,
            game_id: p.game_id,
            period: p.period,
            clock: p.clock,
            game_status: p.period == null ? "final" : "live",
          },
        ])
      );
  
      applyLiveSnapshot(snapshotByPlayerId);
    }, [players, applyLiveSnapshot]);

  return {
    players,
    playersByGame,
    mode,
  };
}
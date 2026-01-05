// useLiveScores.ts
import { useEffect, useRef, useState } from "react";
import EventSource from "react-native-sse";

const API = "https://pulse-mobile-api-763243624328.us-central1.run.app";

type LiveSnapshot = {
  games: any[];
  meta: {
    status: "OK" | "DEGRADED" | "BOOTING";
    server_updated_at?: string;
    source_updated_at?: string;
    consecutive_failures?: number;
    seconds_since_last_good?: number | null;
  };
};

export function useLiveScores() {
  const [snapshot, setSnapshot] = useState<LiveSnapshot | null>(null);
  const [connected, setConnected] = useState(false);

  const esRef = useRef<any>(null);
  const backoffRef = useRef(1000); // start 1s
  const lastMessageAtRef = useRef<number | null>(null);

  const scheduleReconnect = () => {
    const wait = Math.min(30000, backoffRef.current);
    backoffRef.current = Math.min(30000, backoffRef.current * 2);
    setTimeout(connect, wait);
  };

  const connect = () => {
    try { esRef.current?.close?.(); } catch {}

    setConnected(false);

    const es = new EventSource(`${API}/live/scores/stream`);
    esRef.current = es;

    es.addEventListener("open", () => {
      setConnected(true);
      backoffRef.current = 1000;
    });

    es.addEventListener("snapshot", (event: any) => {
      try {
        const data = JSON.parse(event.data);
        lastMessageAtRef.current = Date.now();
        setSnapshot(data);
      } catch {}
    });

    es.addEventListener("error", () => {
      setConnected(false);
      try { es.close(); } catch {}
      scheduleReconnect();
    });
  };

  // Initial connect
  useEffect(() => {
    connect();
    return () => {
      try { esRef.current?.close?.(); } catch {}
    };
  }, []);

  // Watchdog for silent SSE hangs
  useEffect(() => {
    const interval = setInterval(() => {
      if (!lastMessageAtRef.current) return;

      const ageMs = Date.now() - lastMessageAtRef.current;
      if (ageMs > 45_000) {
        setConnected(false);
        try { esRef.current?.close?.(); } catch {}
        scheduleReconnect();
      }
    }, 10_000);

    return () => clearInterval(interval);
  }, []);

  const isStale =
    snapshot?.meta?.status === "DEGRADED" ||
    (lastMessageAtRef.current &&
      Date.now() - lastMessageAtRef.current > 30_000);

  return { snapshot, connected, isStale };
}

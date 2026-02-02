// useLiveScores.ts
import { useEffect, useRef, useState } from "react";

const API = "https://pulse-mobile-api-763243624328.us-central1.run.app";
const POLL_INTERVAL_MS = 60_000;

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
  const [loading, setLoading] = useState(true);
  const pollRef = useRef<NodeJS.Timeout | null>(null);

  const fetchSnapshot = async () => {
    try {
      const res = await fetch(`${API}/live/scores`);
      if (!res.ok) throw new Error("API error");

      const data: LiveSnapshot = await res.json();
      setSnapshot(data);
    } catch (err) {
      console.warn("Live scores fetch failed", err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    // initial fetch
    fetchSnapshot();

    // poll every 20s
    pollRef.current = setInterval(fetchSnapshot, POLL_INTERVAL_MS);

    return () => {
      if (pollRef.current) clearInterval(pollRef.current);
    };
  }, []);

  const isStale = snapshot?.meta?.status === "DEGRADED";

  return { snapshot, loading, isStale };
}

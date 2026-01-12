// hooks/useLiveGameOdds
import { useEffect, useState, useRef } from "react";
import {
  fetchLiveGameOdds,
  LiveGameOdds,
} from "@/lib/liveOdds";

export function useLiveGameOdds(gameId?: number) {
  const [odds, setOdds] = useState<LiveGameOdds[]>([]);
  const [loading, setLoading] = useState(false);
  const lastPayloadRef = useRef<string | null>(null);

  useEffect(() => {
    if (!gameId) return;

    let mounted = true;

    async function load() {
      try {
        const data = await fetchLiveGameOdds(gameId);

        const payloadStr = JSON.stringify(data.odds);

        // ⛔️ NO-OP GUARD (MOST IMPORTANT)
        if (payloadStr === lastPayloadRef.current) {
          return;
        }

        lastPayloadRef.current = payloadStr;

        if (mounted) {
          setLoading(true);
          setOdds(data.odds ?? []);
        }
      } catch (err) {
        console.warn("live game odds error", err);
      } finally {
        if (mounted) {
          setLoading(false);
        }
      }
    }

    load();
    const id = setInterval(load, 30_000);

    return () => {
      mounted = false;
      clearInterval(id);
    };
  }, [gameId]);

  return { odds, loading };
}
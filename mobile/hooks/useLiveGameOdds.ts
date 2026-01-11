// hooks/useLiveGameOdds
import { useEffect, useState } from "react";
import {
  fetchLiveGameOdds,
  LiveGameOdds,
} from "@/lib/liveOdds";

export function useLiveGameOdds(gameId?: number) {
  const [odds, setOdds] = useState<LiveGameOdds[]>([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (!gameId) return;

    let mounted = true;

    async function load() {
      try {
        setLoading(true);
        const data = await fetchLiveGameOdds(gameId);
        if (mounted) setOdds(data.odds ?? []);
      } catch (err) {
        console.warn("live game odds error", err);
      } finally {
        setLoading(false);
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
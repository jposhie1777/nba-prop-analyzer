// hooks/useLivePlayerProps
import { useEffect, useState, useRef } from "react";
import {
  fetchLivePlayerProps,
  LivePlayerProp,
} from "@/lib/liveOdds";

export function useLivePlayerProps(gameId?: number) {
  const [props, setProps] = useState<LivePlayerProp[]>([]);
  const [loading, setLoading] = useState(false);
  const lastPayloadRef = useRef<string | null>(null);

  useEffect(() => {
    if (!gameId) return;

    let mounted = true;

    async function load() {
      try {
        setLoading(true);
        const data = await fetchLivePlayerProps(gameId);

        // prevent unnecessary re-renders
        const payloadStr = JSON.stringify(data.props);
        if (payloadStr === lastPayloadRef.current) return;

        lastPayloadRef.current = payloadStr;
        if (mounted) setProps(data.props ?? []);
      } catch (err) {
        console.warn("live player props error", err);
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

  return { props, loading };
}
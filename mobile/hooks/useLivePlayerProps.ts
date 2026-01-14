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
        const data = await fetchLivePlayerProps(gameId);
    
        // 🔍 DEBUG #1 — API response shape
        console.log(
          "[useLivePlayerProps] fetched",
          gameId,
          {
            count: data?.props?.length,
            sample: data?.props?.slice(0, 3),
          }
        );
    
        const payloadStr = JSON.stringify(data.props);
    
        // ⛔️ no-op update guard
        if (payloadStr === lastPayloadRef.current) {
          console.log("[useLivePlayerProps] payload unchanged, skipping update");
          return;
        }
    
        lastPayloadRef.current = payloadStr;
    
        if (mounted) {
          setLoading(true);
          setProps(data.props ?? []);
        }
      } catch (err) {
        console.warn("live player props error", err);
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

  return { props, loading };
}
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
    
       import murmurhash from "murmurhash";

        /* 🔍 DEBUG — API response shape (SAFE) */
        if (__DEV__) {
          console.log("[useLivePlayerProps] fetched", gameId, {
            count: data?.props?.length ?? 0,
            sample: data?.props
              ?.slice(0, 3)
              .map(p => `${p.player_id}:${p.market}:${p.line}`),
          });
        }
        
        /* ======================================================
           CHANGE DETECTION (NO MASSIVE STRINGIFY)
        ====================================================== */
        
        const payloadHash = murmurhash.v3(
          JSON.stringify(
            data.props.map(p => [
              p.player_id,
              p.market,
              p.line,
              p.book,
              p.snapshot_ts ?? p.ingested_at,
              p.odds?.yes,
              p.odds?.over,
              p.odds?.under,
            ])
          )
        );
        
        // ⛔️ no-op update guard
        if (payloadHash === lastPayloadRef.current) {
          return;
        }
        
        lastPayloadRef.current = payloadHash;
    
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
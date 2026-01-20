// hooks/useLivePlayerProps.ts
import { useEffect, useRef, useState } from "react";
import {
  fetchLivePlayerProps,
  LivePlayerProp,
} from "@/lib/liveOdds";

/* ======================================================
   Tiny, dependency-free hash for change detection
====================================================== */
function fastHash(input: string): number {
  let hash = 0;
  for (let i = 0; i < input.length; i++) {
    hash = (hash << 5) - hash + input.charCodeAt(i);
    hash |= 0; // force 32-bit int
  }
  return hash;
}

export function useLivePlayerProps(gameId?: number) {
  const [props, setProps] = useState<LivePlayerProp[]>([]);
  const [loading, setLoading] = useState(false);

  // Store last payload fingerprint (number, not giant string)
  const lastPayloadRef = useRef<number | null>(null);

  useEffect(() => {
    if (!gameId) return;

    let mounted = true;
    let intervalId: ReturnType<typeof setInterval> | null = null;

    async function load() {
      try {
        setLoading(true);

        const data = await fetchLivePlayerProps(gameId);
        if (!mounted || !data?.props) return;

        /* 🔍 DEBUG — API response shape (SAFE) */
        if (__DEV__) {
          console.log("[useLivePlayerProps] fetched", gameId, {
            count: data.props.length,
            sample: data.props
              .slice(0, 3)
              .map(p => `${p.player_id}:${p.market}:${p.line}`),
          });
        }

        /* ======================================================
           CHANGE DETECTION (HASHED FINGERPRINT)
           Only hash fields that affect rendering
        ====================================================== */
        const fingerprint = fastHash(
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

        // ⛔️ No-op update guard
        if (fingerprint === lastPayloadRef.current) {
          return;
        }

        lastPayloadRef.current = fingerprint;

        // ✅ Safe state update
        if (mounted) {
          setProps(data.props);
        }
      } catch (err) {
        console.warn("[useLivePlayerProps] error", err);
      } finally {
        if (mounted) {
          setLoading(false);
        }
      }
    }

    // Initial load + polling
    load();
    intervalId = setInterval(load, 30_000);

    return () => {
      mounted = false;
      if (intervalId) clearInterval(intervalId);
    };
  }, [gameId]);

  return { props, loading };
}
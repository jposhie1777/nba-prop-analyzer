// hooks/useLivePlayerProps.ts
import { useEffect, useRef } from "react";

import { fetchLivePlayerProps } from "@/lib/liveOdds";
import { adaptPlayerProps } from "@/services/adapters/adaptPlayerProps";
import { useLiveStore } from "@/store/liveStore";

export function useLivePlayerProps(gameId?: number) {
  const lastPayloadRef = useRef<string | null>(null);

  useEffect(() => {
    if (!gameId) return;

    let cancelled = false;

    async function load() {
      try {
        const raw = await fetchLivePlayerProps(gameId);
        if (cancelled) return;

        // 🔒 no-op guard (prevents pointless upserts)
        const payloadStr = JSON.stringify(raw.props);
        if (payloadStr === lastPayloadRef.current) {
          return;
        }
        lastPayloadRef.current = payloadStr;

        // 🔁 ADAPT
        const adaptedPlayers = adaptPlayerProps(raw);

        // 🧠 UPSERT (THIS IS WHERE getState() GOES)
        const store = useLiveStore.getState();
        for (const p of adaptedPlayers) {
          store.upsertPropMarkets(p.gameId, p.playerId, p.markets);
        }
      } catch (err) {
        console.warn("❌ live player props poll failed", err);
      }
    }

    load();
    const id = setInterval(load, 30_000);

    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [gameId]);
}
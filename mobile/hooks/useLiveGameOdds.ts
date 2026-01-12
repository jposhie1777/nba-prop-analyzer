// hooks/useLiveGameOdds.ts
import { useEffect, useRef } from "react";

import { fetchLiveGameOdds } from "@/lib/liveOdds";
import { adaptGameOdds } from "@/services/adapters/adaptGameOdds";
import { useLiveStore } from "@/store/liveStore";

export function useLiveGameOdds(gameId?: number) {
  const lastPayloadRef = useRef<string | null>(null);

  useEffect(() => {
    if (!gameId) return;

    let cancelled = false;

    async function load() {
      try {
        const raw = await fetchLiveGameOdds(gameId);
        if (cancelled) return;

        // 🔒 NO-OP GUARD (prevents pointless upserts / rerenders)
        const payloadStr = JSON.stringify(raw.odds);
        if (payloadStr === lastPayloadRef.current) {
          return;
        }
        lastPayloadRef.current = payloadStr;

        // 🔁 ADAPT
        const adapted = adaptGameOdds(raw);

        // 🧠 UPSERT (this is where getState() belongs)
        useLiveStore.getState().upsertOdds([adapted]);
      } catch (err) {
        console.warn("❌ live game odds poll failed", err);
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
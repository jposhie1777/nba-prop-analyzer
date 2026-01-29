// components/livedatabridge
import { useEffect, useMemo } from "react";

import { useLivePlayerStats } from "@/hooks/useLivePlayerStats";
import { useLiveGames } from "@/hooks/useLiveGames";
import { useParlayTracker } from "@/store/useParlayTracker";
import { buildLiveSnapshot } from "@/services/adapters/liveParlayAdapter";

export function LiveDataBridge() {
  console.log("🔥 LiveDataBridge MOUNTED");
  
  const { players } = useLivePlayerStats();
  const { games } = useLiveGames();

  const applyLiveSnapshot = useParlayTracker(
    (s) => s.applyLiveSnapshot
  );
  const cleanupExpired = useParlayTracker(
    (s) => s.cleanupExpired
  );

  const snapshot = useMemo(() => {
    if (!players.length) return null;

    const snap = buildLiveSnapshot({ players, games });

    return snap;
  }, [players, games]);


  /* ======================================================
     Apply live stats + cleanup when data updates
  ====================================================== */
  useEffect(() => {
    if (snapshot) {
      applyLiveSnapshot(snapshot);
      cleanupExpired(); // 👈 THIS IS THE KEY
    }
  }, [snapshot, applyLiveSnapshot, cleanupExpired]);

  /* ======================================================
     Initial cleanup on mount (cold start / app reopen)
  ====================================================== */
  useEffect(() => {
    cleanupExpired();
  }, [cleanupExpired]);

  return null;
}

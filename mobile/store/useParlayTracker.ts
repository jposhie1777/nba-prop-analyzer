// store/useParlayTracker.ts
import { create } from "zustand";
import { persist, createJSONStorage } from "zustand/middleware";
import AsyncStorage from "@react-native-async-storage/async-storage";

/* ======================================================
   TYPES
====================================================== */

/**
 * Canonical market keys only
 * MUST match live stat fields
 */
export type StatMarket = "pts" | "reb" | "ast" | "fg3m";

export type TrackedParlayLeg = {
  leg_id: string;

  /* ---------- Identity ---------- */
  player_id: number;
  player_name: string;

  /* ---------- Bet Definition ---------- */
  market: StatMarket;
  side: "over" | "under";
  line: number;
  odds: number;

  /* ---------- Live Runtime ---------- */
  current?: number;

  period?: number | null;
  clock?: string | null;
  game_status?: "pregame" | "live" | "final";

  status?: "pending" | "winning" | "losing" | "pushed";
  isFinal?: boolean;
};

export type TrackedParlaySnapshot = {
  parlay_id: string;
  created_at: string;
  source: "toggle" | "copy" | "gambly";

  legs: TrackedParlayLeg[];

  stake: number;
  parlay_odds: number | null;
  payout: number | null;
};

/* ======================================================
   LIVE SNAPSHOT (RUNTIME ONLY)
====================================================== */

export type LiveSnapshotByPlayerId = Record<
  number,
  {
    pts?: number;
    reb?: number;
    ast?: number;
    fg3m?: number;

    game_id?: number;
    period?: number | null;
    clock?: string | null;
    game_status?: "pregame" | "live" | "final";
  }
>;

/* ======================================================
   STORE STATE
====================================================== */

type State = {
  tracked: Record<string, TrackedParlaySnapshot>;

  track: (snapshot: TrackedParlaySnapshot) => void;
  untrack: (parlay_id: string) => void;
  clearAll: () => void;

  isTracked: (parlay_id: string) => boolean;

  applyLiveSnapshot: (
    snapshotByPlayerId: LiveSnapshotByPlayerId
  ) => void;

  cleanupExpired: () => void;
};

/* ======================================================
   STORE (PERSISTED)
====================================================== */

export const useParlayTracker = create<State>()(
  persist(
    (set, get) => ({
      tracked: {},

      /* ---------- Add / Replace Parlay ---------- */
      track: (snapshot) =>
        set((state) => ({
          tracked: {
            ...state.tracked,
            [snapshot.parlay_id]: {
              ...snapshot,
              legs: snapshot.legs.map((leg) => ({
                ...leg,
                status: leg.status ?? "pending",
              })),
            },
          },
        })),

      /* ---------- Remove Single Parlay ---------- */
      untrack: (parlay_id) =>
        set((state) => {
          const next = { ...state.tracked };
          delete next[parlay_id];
          return { tracked: next };
        }),

      /* ---------- Remove All Parlays ---------- */
      clearAll: () => set({ tracked: {} }),

      /* ---------- Lookup ---------- */
      isTracked: (parlay_id) =>
        Boolean(get().tracked[parlay_id]),

      /* ======================================================
         LIVE ENRICHMENT
      ====================================================== */
      applyLiveSnapshot: (snapshotByPlayerId) => {
        console.log(
          "🟢 APPLY LIVE SNAPSHOT",
          Object.keys(snapshotByPlayerId).length,
          "players"
        );

        set((state) => ({
          tracked: Object.fromEntries(
            Object.entries(state.tracked).map(([parlayId, parlay]) => [
              parlayId,
              {
                ...parlay,
                legs: parlay.legs.map((leg) => {
                  const live =
                    snapshotByPlayerId[Number(leg.player_id)];
                  if (!live) return leg;

                  /* ---------- Current stat ---------- */
                  const current =
                    leg.market === "pts" ? live.pts :
                    leg.market === "reb" ? live.reb :
                    leg.market === "ast" ? live.ast :
                    leg.market === "fg3m" ? live.fg3m :
                    undefined;

                  /* ---------- Game status ---------- */
                  const gameStatus =
                    live.game_status ??
                    leg.game_status ??
                    "live";

                  /* ---------- Resolve status ---------- */
                  let status = leg.status ?? "pending";

                  if (current != null && gameStatus !== "final") {
                    status =
                      leg.side === "over"
                        ? current >= leg.line
                          ? "winning"
                          : "pending"
                        : current <= leg.line
                        ? "winning"
                        : "pending";
                  }

                  if (gameStatus === "final" && current != null) {
                    status =
                      leg.side === "over"
                        ? current > leg.line
                          ? "winning"
                          : current < leg.line
                          ? "losing"
                          : "pushed"
                        : current < leg.line
                        ? "winning"
                        : current > leg.line
                        ? "losing"
                        : "pushed";
                  }

                  return {
                    ...leg,
                    current: current ?? leg.current,
                    period: live.period ?? leg.period,
                    clock: live.clock ?? leg.clock,
                    game_status: gameStatus,
                    status,
                    isFinal: gameStatus === "final",
                  };
                }),
              },
            ])
          ),
        }));
      },

      /* ======================================================
         CLEANUP FINALIZED PARLAYS
      ====================================================== */
      cleanupExpired: () =>
        set((state) => {
          const next = Object.fromEntries(
            Object.entries(state.tracked).filter(
              ([_, parlay]) =>
                // keep parlays with at least one non-final leg
                parlay.legs.some((leg) => !leg.isFinal)
            )
          );

          return { tracked: next };
        }),
    }),
    {
      name: "pulse-tracked-parlays",
      version: 2,
      storage: createJSONStorage(() => AsyncStorage),
    }
  )
);

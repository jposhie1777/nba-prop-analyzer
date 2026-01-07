// /lib/dev/devStore.ts
import { create } from "zustand";
import * as Clipboard from "expo-clipboard";
import AsyncStorage from "@react-native-async-storage/async-storage";

/* --------------------------------------------------
   CONSTANTS
-------------------------------------------------- */
const MAX_NETWORK_ITEMS = 50;
const MAX_ERROR_ITEMS = 30;

const DEV_FLAGS_STORAGE_KEY = "__DEV_FLAGS__";

/* 🔴 NEW: DEV UNLOCK CONFIG */
const DEV_UNLOCK_TAPS_REQUIRED = 7;
const DEV_UNLOCK_TAP_WINDOW_MS = 2000;

/* --------------------------------------------------
   TYPES
-------------------------------------------------- */
type NetworkLog = {
  id: string;
  ts: number;
  method: string;
  url: string;
  status?: number;
  ms?: number;
  error?: string;
};

type ErrorLog = {
  id: string;
  ts: number;
  name: string;
  message: string;
  stack?: string;
};

type HealthCheck = {
  key: string;
  label: string;
  url: string;
  lastStatus?: number;
  lastMs?: number;
  lastOkTs?: number;
  error?: string;
};

type SSEStatus = {
  connected: boolean;
  eventCount: number;
  lastEventTs?: number;
  lastError?: string;
};

type DataFreshness = {
  key: string;
  label: string;
  lastUpdatedTs?: number;
  rowCount?: number;
  error?: string;
};

/* --------------------------------------------------
   STORE SHAPE
-------------------------------------------------- */
type DevStore = {
  /* 🔴 NEW: DEV LOCK */
  devUnlocked: boolean;
  unlockTaps: number;
  lastUnlockTapTs?: number;

  network: {
    items: NetworkLog[];
    maxItems: number;
  };

  errors: {
    items: ErrorLog[];
    maxItems: number;
  };

  flags: {
    values: Record<string, boolean>;
  };

  health: {
    checks: HealthCheck[];
  };

  sse: SSEStatus;

  freshness: {
    datasets: DataFreshness[];
  };

  actions: {
    logNetwork: (entry: Omit<NetworkLog, "id" | "ts">) => void;
    logError: (err: Error | string) => void;

    clearNetwork: () => void;
    clearErrors: () => void;

    toggleFlag: (key: string) => void;

    hydrateFlags: () => Promise<void>;
    persistFlags: () => Promise<void>;

    /* 🔴 NEW: DEV LOCK ACTIONS */
    registerDevTap: () => void;   // B: tap anywhere
    unlockDev: () => void;        // C: long press title
    resetDevUnlock: () => void;

    copyDevReport: (
      section?:
        | "network"
        | "errors"
        | "flags"
        | "health"
        | "sse"
        | "freshness"
    ) => void;

    testCrash: () => never;

    runHealthCheck: (key: string) => Promise<void>;
    runAllHealthChecks: () => Promise<void>;

    reportSSEConnect: () => void;
    reportSSEDisconnect: (err?: string) => void;
    reportSSEEvent: () => void;

    updateFreshness: (
      key: string,
      data: {
        lastUpdatedTs?: number;
        rowCount?: number;
        error?: string;
      }
    ) => void;

    fetchFreshness: (key: string) => Promise<void>;
  };
};

/* --------------------------------------------------
   STORE IMPLEMENTATION
-------------------------------------------------- */
export const useDevStore = create<DevStore>((set, get) => ({
  /* 🔴 NEW: DEV LOCK */
  devUnlocked: false,
  unlockTaps: 0,
  lastUnlockTapTs: undefined,

  network: {
    items: [],
    maxItems: MAX_NETWORK_ITEMS,
  },

  errors: {
    items: [],
    maxItems: MAX_ERROR_ITEMS,
  },

  flags: {
    values: {
      ENABLE_LIVE_GAMES: true,
      ENABLE_ODDS: true,
      USE_MOCK_DATA: false,
      DEBUG_UI: false,
    },
  },

  health: {
    checks: [
      { key: "health", label: "API Health", url: "/health" },
      { key: "live", label: "Live Scores Debug", url: "/live/scores/debug" },
      { key: "props", label: "Props", url: "/props" },
    ],
  },

  sse: {
    connected: false,
    eventCount: 0,
  },

  freshness: {
    datasets: [
      { key: "live_games", label: "Live Games" },
      { key: "props", label: "Props" },
      { key: "player_stats", label: "Player Stats" },
    ],
  },

  actions: {
    /* ---------------- NETWORK ---------------- */
    logNetwork(entry) {
      set((state) => ({
        network: {
          ...state.network,
          items: [
            {
              id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
              ts: Date.now(),
              ...entry,
            },
            ...state.network.items,
          ].slice(0, state.network.maxItems),
        },
      }));
    },

    /* ---------------- ERRORS ---------------- */
    logError(err) {
      const error =
        typeof err === "string"
          ? new Error(err)
          : err instanceof Error
          ? err
          : new Error("Unknown error");

      set((state) => ({
        errors: {
          ...state.errors,
          items: [
            {
              id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
              ts: Date.now(),
              name: error.name || "Error",
              message: error.message || String(error),
              stack: error.stack,
            },
            ...state.errors.items,
          ].slice(0, state.errors.maxItems),
        },
      }));
    },

    clearNetwork() {
      set((state) => ({
        network: { ...state.network, items: [] },
      }));
    },

    clearErrors() {
      set((state) => ({
        errors: { ...state.errors, items: [] },
      }));
    },

    /* ---------------- FLAGS ---------------- */
    toggleFlag(key) {
      set((state) => ({
        flags: {
          values: {
            ...state.flags.values,
            [key]: !state.flags.values[key],
          },
        },
      }));

      get().actions.persistFlags();
    },

    async hydrateFlags() {
      try {
        const raw = await AsyncStorage.getItem(DEV_FLAGS_STORAGE_KEY);
        if (!raw) return;

        const parsed = JSON.parse(raw);
        if (typeof parsed !== "object" || !parsed) return;

        set((state) => ({
          flags: {
            values: {
              ...state.flags.values,
              ...parsed,
            },
          },
        }));
      } catch {
        // dev-only, ignore
      }
    },

    async persistFlags() {
      try {
        await AsyncStorage.setItem(
          DEV_FLAGS_STORAGE_KEY,
          JSON.stringify(get().flags.values)
        );
      } catch {
        // dev-only, ignore
      }
    },

    /* ---------------- 🔴 DEV LOCK ---------------- */
    registerDevTap() {
      const now = Date.now();
      const { devUnlocked, unlockTaps, lastUnlockTapTs } = get();

      if (devUnlocked) return;

      const withinWindow =
        typeof lastUnlockTapTs === "number" &&
        now - lastUnlockTapTs <= DEV_UNLOCK_TAP_WINDOW_MS;

      const nextTaps = withinWindow ? unlockTaps + 1 : 1;

      if (nextTaps >= DEV_UNLOCK_TAPS_REQUIRED) {
        set({
          devUnlocked: true,
          unlockTaps: 0,
          lastUnlockTapTs: undefined,
        });
        return;
      }

      set({
        unlockTaps: nextTaps,
        lastUnlockTapTs: now,
      });
    },

    unlockDev() {
      set({
        devUnlocked: true,
        unlockTaps: 0,
        lastUnlockTapTs: undefined,
      });
    },

    resetDevUnlock() {
      set({
        devUnlocked: false,
        unlockTaps: 0,
        lastUnlockTapTs: undefined,
      });
    },

    /* ---------------- REPORT ---------------- */
    async copyDevReport(section) {
      const s = get();
      const payload =
        section === "network"
          ? s.network.items
          : section === "errors"
          ? s.errors.items
          : section === "flags"
          ? s.flags.values
          : section === "health"
          ? s.health.checks
          : section === "sse"
          ? s.sse
          : section === "freshness"
          ? s.freshness.datasets
          : {
              network: s.network.items,
              errors: s.errors.items,
              flags: s.flags.values,
              health: s.health.checks,
              sse: s.sse,
              freshness: s.freshness.datasets,
            };

      await Clipboard.setStringAsync(JSON.stringify(payload, null, 2));
    },

    testCrash() {
      throw new Error("💥 Dev crash test triggered");
    },

    /* ---------------- HEALTH ---------------- */
    async runHealthCheck(key) {
      const check = get().health.checks.find((c) => c.key === key);
      if (!check) return;

      try {
        const start = Date.now();
        const res = await fetch(check.url);
        const ms = Date.now() - start;

        set((state) => ({
          health: {
            checks: state.health.checks.map((c) =>
              c.key === key
                ? {
                    ...c,
                    lastStatus: res.status,
                    lastMs: ms,
                    lastOkTs: res.ok ? Date.now() : c.lastOkTs,
                    error: res.ok ? undefined : `HTTP ${res.status}`,
                  }
                : c
            ),
          },
        }));
      } catch (err: any) {
        set((state) => ({
          health: {
            checks: state.health.checks.map((c) =>
              c.key === key
                ? { ...c, error: err?.message ?? "Network error" }
                : c
            ),
          },
        }));
      }
    },

    async runAllHealthChecks() {
      for (const c of get().health.checks) {
        await get().actions.runHealthCheck(c.key);
      }
    },

    /* ---------------- SSE ---------------- */
    reportSSEConnect() {
      set(() => ({
        sse: { connected: true, eventCount: 0, lastError: undefined },
      }));
    },

    reportSSEDisconnect(err) {
      set((state) => ({
        sse: { ...state.sse, connected: false, lastError: err },
      }));
    },

    reportSSEEvent() {
      set((state) => ({
        sse: {
          ...state.sse,
          eventCount: state.sse.eventCount + 1,
          lastEventTs: Date.now(),
        },
      }));
    },

    /* ---------------- DATA FRESHNESS ---------------- */
    updateFreshness(key, data) {
      set((state) => ({
        freshness: {
          datasets: state.freshness.datasets.map((d) =>
            d.key === key ? { ...d, ...data } : d
          ),
        },
      }));
    },

    async fetchFreshness(key) {
      try {
        const res = await fetch(`/debug/freshness/${key}`);
        const json = await res.json();

        get().actions.updateFreshness(key, {
          lastUpdatedTs: json.last_updated_ts
            ? new Date(json.last_updated_ts).getTime()
            : undefined,
          rowCount: json.row_count,
        });
      } catch (err: any) {
        get().actions.updateFreshness(key, {
          error: err?.message ?? "Failed to fetch freshness",
        });
      }
    },
  },
}));
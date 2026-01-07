// /lib/dev/devStore.ts
import { create } from "zustand";
import * as Clipboard from "expo-clipboard";

/* --------------------------------------------------
   CONSTANTS
-------------------------------------------------- */
const MAX_NETWORK_ITEMS = 50;
const MAX_ERROR_ITEMS = 30;

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

/* HEALTH CHECK TYPE */
type HealthCheck = {
  key: string;
  label: string;
  url: string;
  lastStatus?: number;
  lastMs?: number;
  lastOkTs?: number;
  error?: string;
};

/* SSE STATUS TYPE */
type SSEStatus = {
  connected: boolean;
  eventCount: number;
  lastEventTs?: number;
  lastError?: string;
};

/* 🔴 NEW: DATA FRESHNESS TYPE */
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
  /* ---------------- NETWORK ---------------- */
  network: {
    items: NetworkLog[];
    maxItems: number;
  };

  /* ---------------- ERRORS ---------------- */
  errors: {
    items: ErrorLog[];
    maxItems: number;
  };

  /* ---------------- FLAGS ---------------- */
  flags: {
    values: Record<string, boolean>;
  };

  /* ---------------- HEALTH ---------------- */
  health: {
    checks: HealthCheck[];
  };

  /* ---------------- SSE ---------------- */
  sse: SSEStatus;

  /* 🔴 NEW: DATA FRESHNESS ---------------- */
  freshness: {
    datasets: DataFreshness[];
  };

  /* ---------------- ACTIONS ---------------- */
  actions: {
    logNetwork: (entry: Omit<NetworkLog, "id" | "ts">) => void;
    logError: (err: Error | string) => void;

    clearNetwork: () => void;
    clearErrors: () => void;

    toggleFlag: (key: string) => void;

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

    /* HEALTH */
    runHealthCheck: (key: string) => Promise<void>;
    runAllHealthChecks: () => Promise<void>;

    /* SSE */
    reportSSEConnect: () => void;
    reportSSEDisconnect: (err?: string) => void;
    reportSSEEvent: () => void;

    /* 🔴 NEW: DATA FRESHNESS */
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
  /* ---------------- NETWORK ---------------- */
  network: {
    items: [],
    maxItems: MAX_NETWORK_ITEMS,
  },

  /* ---------------- ERRORS ---------------- */
  errors: {
    items: [],
    maxItems: MAX_ERROR_ITEMS,
  },

  /* ---------------- FLAGS ---------------- */
  flags: {
    values: {
      ENABLE_LIVE_GAMES: true,
      ENABLE_ODDS: true,
      USE_MOCK_DATA: false,
      DEBUG_UI: false,
    },
  },

  /* ---------------- HEALTH ---------------- */
  health: {
    checks: [
      { key: "health", label: "API Health", url: "/health" },
      { key: "live", label: "Live Scores Debug", url: "/live/scores/debug" },
      { key: "props", label: "Props", url: "/props" },
    ],
  },

  /* ---------------- SSE ---------------- */
  sse: {
    connected: false,
    eventCount: 0,
  },

  /* 🔴 NEW: DATA FRESHNESS ---------------- */
  freshness: {
    datasets: [
      { key: "live_games", label: "Live Games" },
      { key: "props", label: "Props" },
      { key: "player_stats", label: "Player Stats" },
    ],
  },

  /* ---------------- ACTIONS ---------------- */
  actions: {
    /* NETWORK */
    logNetwork(entry) {
      set((state) => {
        const next: NetworkLog = {
          id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
          ts: Date.now(),
          ...entry,
        };

        return {
          network: {
            ...state.network,
            items: [next, ...state.network.items].slice(
              0,
              state.network.maxItems
            ),
          },
        };
      });
    },

    /* ERRORS */
    logError(err) {
      const error =
        typeof err === "string"
          ? new Error(err)
          : err instanceof Error
          ? err
          : new Error("Unknown error");

      set((state) => {
        const next: ErrorLog = {
          id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
          ts: Date.now(),
          name: error.name || "Error",
          message: error.message || String(error),
          stack: error.stack,
        };

        return {
          errors: {
            ...state.errors,
            items: [next, ...state.errors.items].slice(
              0,
              state.errors.maxItems
            ),
          },
        };
      });
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

    toggleFlag(key) {
      set((state) => ({
        flags: {
          values: {
            ...state.flags.values,
            [key]: !state.flags.values[key],
          },
        },
      }));
    },

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

    /* HEALTH */
    async runHealthCheck(key) {
      const check = get().health.checks.find((c) => c.key === key);
      if (!check) return;

      const start = Date.now();

      try {
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

    /* SSE */
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

    /* 🔴 DATA FRESHNESS */
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
          error: undefined,
        });
      } catch (err: any) {
        get().actions.updateFreshness(key, {
          error: err?.message ?? "Failed to fetch freshness",
        });
      }
    },
  },
}));
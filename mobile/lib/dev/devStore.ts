// /lib/dev/devStore.ts
import { create } from "zustand";
import * as Clipboard from "expo-clipboard";
import AsyncStorage from "@react-native-async-storage/async-storage";
import Constants from "expo-constants";

/* --------------------------------------------------
   CONSTANTS
-------------------------------------------------- */
const MAX_NETWORK_ITEMS = 50;
const MAX_ERROR_ITEMS = 30;

const DEV_FLAGS_STORAGE_KEY = "__DEV_FLAGS__";
const GITHUB_PAT_STORAGE_KEY = "__DEV_GITHUB_PAT__";
const GITHUB_REPO = "jposhie1777/nba-prop-analyzer";

/* 🔴 NEW: DEV UNLOCK CONFIG */
const DEV_UNLOCK_TAPS_REQUIRED = 4;
const DEV_UNLOCK_TAP_WINDOW_MS = 2000;
const API_URL =
  Constants.expoConfig?.extra?.API_URL ??
  // @ts-ignore
  Constants.manifest?.extra?.API_URL ??
  "";
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

type WorkflowTrigger = {
  id: string;
  label: string;
  status: "idle" | "loading" | "success" | "error";
  lastTriggeredTs?: number;
  error?: string;
};

type SpTrigger = {
  id: string;
  label: string;
  call: string;
  status: "idle" | "loading" | "success" | "error";
  lastTriggeredTs?: number;
  jobId?: string;
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

  githubPat: string;

  workflows: {
    triggers: WorkflowTrigger[];
  };

  spTriggers: SpTrigger[];

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

    setGithubPat: (pat: string) => Promise<void>;
    hydrateGithubPat: () => Promise<void>;
    triggerWorkflow: (id: string, inputs?: Record<string, string>) => Promise<void>;
    runSp: (id: string) => Promise<void>;
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
      {
        key: "health",
        label: "API Health",
        url: API_URL ? `${API_URL}/health` : "/health",
      },
      {
        key: "live",
        label: "Live Scores Debug",
        url: API_URL ? `${API_URL}/live/scores/debug` : "/live/scores/debug",
      },
      {
        key: "props",
        label: "Props",
        url: API_URL ? `${API_URL}/props` : "/props",
      },
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

  githubPat: "",

  spTriggers: [
    {
      id: "epl_team_pipeline",
      label: "EPL Team Pipeline",
      call: "CALL epl_data.sp_build_epl_team_pipeline();",
      status: "idle",
    },
    {
      id: "mls_fact_tables",
      label: "MLS All Fact Tables",
      call: "CALL `mls_data.sp_build_all_fact_tables`();",
      status: "idle",
    },
    {
      id: "epl_betting_pipeline",
      label: "EPL Betting Pipeline",
      call: "CALL soccer_data.run_epl_betting_pipeline();",
      status: "idle",
    },
  ],

  workflows: {
    triggers: [
      { id: "epl_daily_loader.yml", label: "EPL Daily Loader", status: "idle" },
      { id: "mls_daily.yml", label: "MLS Daily Loader", status: "idle" },
      { id: "atp_daily_loader.yml", label: "ATP Daily Loader", status: "idle" },
      { id: "atp_odds_daily.yml", label: "ATP Daily Odds", status: "idle" },
      { id: "sheets_bq_sync.yml", label: "ATP Sheets → BQ", status: "idle" },
      { id: "pga_daily_ingest.yml", label: "PGA Daily Loader", status: "idle" },
      { id: "pga_odds_daily.yml", label: "PGA Daily Odds", status: "idle" },
      { id: "soccer_odds_sheets_sync.yml", label: "Soccer Odds Sync", status: "idle" },
      { id: "epl_backfill.yml", label: "EPL Backfill", status: "idle" },
      { id: "mls_backfill.yml", label: "MLS Backfill", status: "idle" },
      { id: "atp_backfill.yml", label: "ATP Backfill", status: "idle" },
      { id: "pga_backfill.yml", label: "PGA Backfill", status: "idle" },
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
        const url = `${API_URL}/debug/freshness/${key}`;

        const start = Date.now();
        const res = await fetch(url);
        const text = await res.text();

        if (!res.ok) {
          throw new Error(`HTTP ${res.status}: ${text.slice(0, 120)}`);
        }

        const json = JSON.parse(text);

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

    /* ---------------- STORED PROCEDURES ---------------- */
    async runSp(id) {
      set((state) => ({
        spTriggers: state.spTriggers.map((s) =>
          s.id === id ? { ...s, status: "loading", error: undefined, jobId: undefined } : s
        ),
      }));

      const sp = get().spTriggers.find((s) => s.id === id);
      if (!sp) return;

      try {
        const res = await fetch(`${API_URL}/dev/bq/run-sp`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ call: sp.call }),
        });

        const json = await res.json();

        if (json.status === "started") {
          set((state) => ({
            spTriggers: state.spTriggers.map((s) =>
              s.id === id
                ? { ...s, status: "success", lastTriggeredTs: Date.now(), jobId: json.job_id, error: undefined }
                : s
            ),
          }));
        } else {
          throw new Error(json.error ?? "Unknown error");
        }
      } catch (err: any) {
        set((state) => ({
          spTriggers: state.spTriggers.map((s) =>
            s.id === id
              ? { ...s, status: "error", error: err?.message ?? "Failed to run SP" }
              : s
          ),
        }));
      }
    },

    /* ---------------- GITHUB PAT ---------------- */
    async setGithubPat(pat) {
      set({ githubPat: pat });
      try {
        await AsyncStorage.setItem(GITHUB_PAT_STORAGE_KEY, pat);
      } catch {
        // dev-only, ignore
      }
    },

    async hydrateGithubPat() {
      try {
        const pat = await AsyncStorage.getItem(GITHUB_PAT_STORAGE_KEY);
        if (pat) set({ githubPat: pat });
      } catch {
        // dev-only, ignore
      }
    },

    /* ---------------- WORKFLOW TRIGGERS ---------------- */
    async triggerWorkflow(id, inputs) {
      const { githubPat } = get();

      if (!githubPat) {
        set((state) => ({
          workflows: {
            triggers: state.workflows.triggers.map((t) =>
              t.id === id
                ? { ...t, status: "error", error: "No GitHub PAT set — enter one in GitHub Auth above" }
                : t
            ),
          },
        }));
        return;
      }

      set((state) => ({
        workflows: {
          triggers: state.workflows.triggers.map((t) =>
            t.id === id ? { ...t, status: "loading", error: undefined } : t
          ),
        },
      }));

      try {
        const res = await fetch(
          `https://api.github.com/repos/${GITHUB_REPO}/actions/workflows/${id}/dispatches`,
          {
            method: "POST",
            headers: {
              Authorization: `Bearer ${githubPat}`,
              Accept: "application/vnd.github+json",
              "Content-Type": "application/json",
              "X-GitHub-Api-Version": "2022-11-28",
            },
            body: JSON.stringify({ ref: "master", inputs: inputs ?? {} }),
          }
        );

        if (res.status === 204 || res.ok) {
          set((state) => ({
            workflows: {
              triggers: state.workflows.triggers.map((t) =>
                t.id === id
                  ? { ...t, status: "success", lastTriggeredTs: Date.now(), error: undefined }
                  : t
              ),
            },
          }));
        } else {
          const text = await res.text().catch(() => `HTTP ${res.status}`);
          throw new Error(`HTTP ${res.status}: ${text.slice(0, 120)}`);
        }
      } catch (err: any) {
        set((state) => ({
          workflows: {
            triggers: state.workflows.triggers.map((t) =>
              t.id === id
                ? { ...t, status: "error", error: err?.message ?? "Failed to trigger workflow" }
                : t
            ),
          },
        }));
      }
    },
  },
}));

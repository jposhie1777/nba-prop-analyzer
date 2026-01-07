// /lib/dev/devStore.ts
import { create } from "zustand";
import * as Clipboard from "expo-clipboard";

const MAX_NETWORK_ITEMS = 50;
const MAX_ERROR_ITEMS = 30;

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

  /* ---------------- ACTIONS ---------------- */
  actions: {
    logNetwork: (entry: Omit<NetworkLog, "id" | "ts">) => void;
    logError: (err: Error | string) => void;

    clearNetwork: () => void;
    clearErrors: () => void;

    toggleFlag: (key: string) => void;

    copyDevReport: (section?: "network" | "errors" | "flags") => void;

    testCrash: () => never;
  };
};

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

  /* ---------------- ACTIONS ---------------- */
  actions: {
    logNetwork(entry) {
      set((state) => {
        const next: NetworkLog = {
          id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
          ts: Date.now(),
          ...entry,
        };

        const items = [next, ...state.network.items].slice(
          0,
          state.network.maxItems
        );

        return {
          network: {
            ...state.network,
            items,
          },
        };
      });
    },

    logError(err) {
      const error =
        typeof err === "string" ? new Error(err) : err instanceof Error ? err : new Error("Unknown error");

      set((state) => {
        const next: ErrorLog = {
          id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
          ts: Date.now(),
          name: error.name || "Error",
          message: error.message || String(error),
          stack: error.stack,
        };

        const items = [next, ...state.errors.items].slice(
          0,
          state.errors.maxItems
        );

        return {
          errors: {
            ...state.errors,
            items,
          },
        };
      });
    },

    clearNetwork() {
      set((state) => ({
        network: {
          ...state.network,
          items: [],
        },
      }));
    },

    clearErrors() {
      set((state) => ({
        errors: {
          ...state.errors,
          items: [],
        },
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
      const state = get();

      let payload: any;

      switch (section) {
        case "network":
          payload = state.network.items;
          break;
        case "errors":
          payload = state.errors.items;
          break;
        case "flags":
          payload = state.flags.values;
          break;
        default:
          payload = {
            network: state.network.items,
            errors: state.errors.items,
            flags: state.flags.values,
          };
      }

      await Clipboard.setStringAsync(
        JSON.stringify(payload, null, 2)
      );
    },

    testCrash() {
      throw new Error("💥 Dev crash test triggered");
    },
  },
}));
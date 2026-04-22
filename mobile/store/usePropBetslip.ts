// store/usePropBetslip.ts
import { create } from "zustand";
import AsyncStorage from "@react-native-async-storage/async-storage";

const KEY = "prop_betslip_v1";

export type PropSlipItem = {
  id: string;

  player_id: number;        // ✅ ADD THIS
  player: string;

  market: string;
  side: "over" | "under";
  line: number;
  odds: number;

  matchup?: string;
  sport?: string;
  bookmaker?: string | null;
  dk_event_id?: string | null;
  dk_outcome_code?: string | null;
  dk_odds?: number | null;
  fd_market_id?: string | null;
  fd_selection_id?: string | null;
  fd_odds?: number | null;
};


type State = {
  items: PropSlipItem[];
  add: (item: PropSlipItem) => void;
  remove: (id: string) => void;
  clear: () => void;
  hydrate: () => Promise<void>;
};

export const usePropBetslip = create<State>((set, get) => ({
  items: [],

  hydrate: async () => {
    const raw = await AsyncStorage.getItem(KEY);
    if (!raw) return;
    set({ items: JSON.parse(raw) });
  },

  add: (item) =>
    set((s) => {
      if (s.items.some((i) => i.id === item.id)) return s;
      const next = [...s.items, item];
      AsyncStorage.setItem(KEY, JSON.stringify(next));
      return { items: next };
    }),

  remove: (id) =>
    set((s) => {
      const next = s.items.filter((i) => i.id !== id);
      AsyncStorage.setItem(KEY, JSON.stringify(next));
      return { items: next };
    }),

  clear: () => {
    AsyncStorage.setItem(KEY, "[]");
    set({ items: [] });
  },
}));
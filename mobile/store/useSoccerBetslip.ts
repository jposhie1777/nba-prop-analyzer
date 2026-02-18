import AsyncStorage from "@react-native-async-storage/async-storage";
import { create } from "zustand";

const KEY = "soccer_betslip_v1";

export type SoccerSlipItem = {
  id: string;
  league: string;
  game: string;
  start_time_et?: string;
  market: string;
  outcome: string;
  line?: number | null;
  price: number;
  bookmaker: string;
  rationale?: string;
};

type State = {
  items: SoccerSlipItem[];
  hydrate: () => Promise<void>;
  add: (item: SoccerSlipItem) => void;
  remove: (id: string) => void;
  clear: () => void;
};

export const useSoccerBetslip = create<State>((set) => ({
  items: [],

  hydrate: async () => {
    const raw = await AsyncStorage.getItem(KEY);
    if (!raw) return;
    set({ items: JSON.parse(raw) });
  },

  add: (item) =>
    set((state) => {
      if (state.items.some((existing) => existing.id === item.id)) return state;
      const next = [...state.items, item];
      AsyncStorage.setItem(KEY, JSON.stringify(next));
      return { items: next };
    }),

  remove: (id) =>
    set((state) => {
      const next = state.items.filter((item) => item.id !== id);
      AsyncStorage.setItem(KEY, JSON.stringify(next));
      return { items: next };
    }),

  clear: () => {
    AsyncStorage.setItem(KEY, "[]");
    set({ items: [] });
  },
}));


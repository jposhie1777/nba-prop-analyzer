import AsyncStorage from "@react-native-async-storage/async-storage";
import { create } from "zustand";

const KEY = "pga_betslip_v1";

export type PgaSlipItem = {
  id: string;
  playerId: string | null;
  playerLastName: string;
  playerDisplayName: string;
  groupPlayers: string[];
  tournamentId?: string;
  teeTime?: string;
  roundNumber?: number;
  createdAt: string;
};

type State = {
  items: PgaSlipItem[];
  hydrate: () => Promise<void>;
  add: (item: PgaSlipItem) => void;
  remove: (id: string) => void;
  clear: () => void;
};

export const usePgaBetslip = create<State>((set) => ({
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

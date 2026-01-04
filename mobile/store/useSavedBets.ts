import { create } from "zustand";
import AsyncStorage from "@react-native-async-storage/async-storage";

const STORAGE_KEY = "saved_props_v1";

type SavedBetsStore = {
  savedIds: Set<string>;
  toggleSave: (id: string) => void;
  clearAll: () => void;
  hydrate: () => Promise<void>;
};

export const useSavedBets = create<SavedBetsStore>((set, get) => ({
  savedIds: new Set(),

  toggleSave: (id) => {
    const next = new Set(get().savedIds);
    next.has(id) ? next.delete(id) : next.add(id);
    set({ savedIds: next });
    AsyncStorage.setItem(STORAGE_KEY, JSON.stringify(Array.from(next)));
  },

  clearAll: () => {
    set({ savedIds: new Set() });
    AsyncStorage.removeItem(STORAGE_KEY);
  },

  hydrate: async () => {
    const raw = await AsyncStorage.getItem(STORAGE_KEY);
    if (!raw) return;
    set({ savedIds: new Set(JSON.parse(raw)) });
  },
}));
import { create } from "zustand";
import AsyncStorage from "@react-native-async-storage/async-storage";

const SAVED_PROPS_KEY = "saved_props_v1";

type SavedPropsState = {
  savedIds: Set<string>;
  toggleSave: (id: string) => void;
  clearAll: () => void;
  hydrate: () => Promise<void>;
};

export const useSavedProps = create<SavedPropsState>((set, get) => ({
  savedIds: new Set(),

  hydrate: async () => {
    const raw = await AsyncStorage.getItem(SAVED_PROPS_KEY);
    if (!raw) return;
    set({ savedIds: new Set(JSON.parse(raw)) });
  },

  toggleSave: (id) =>
    set((state) => {
      const next = new Set(state.savedIds);
      next.has(id) ? next.delete(id) : next.add(id);
      AsyncStorage.setItem(SAVED_PROPS_KEY, JSON.stringify([...next]));
      return { savedIds: next };
    }),

  clearAll: () => {
    AsyncStorage.setItem(SAVED_PROPS_KEY, "[]");
    set({ savedIds: new Set() });
  },
}));

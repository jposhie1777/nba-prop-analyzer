import { create } from "zustand";
import AsyncStorage from "@react-native-async-storage/async-storage";

const KEY = "user-settings";

type UserSettings = {
  /** Two-letter US state for FanDuel sportsbook (e.g. "nj", "pa") */
  fdState: string;
  setFdState: (state: string) => void;
  _hydrated: boolean;
  hydrate: () => Promise<void>;
};

export const FD_STATES = [
  { code: "az", label: "Arizona" },
  { code: "co", label: "Colorado" },
  { code: "ct", label: "Connecticut" },
  { code: "dc", label: "Washington DC" },
  { code: "ia", label: "Iowa" },
  { code: "il", label: "Illinois" },
  { code: "in", label: "Indiana" },
  { code: "ky", label: "Kentucky" },
  { code: "la", label: "Louisiana" },
  { code: "ma", label: "Massachusetts" },
  { code: "md", label: "Maryland" },
  { code: "mi", label: "Michigan" },
  { code: "nc", label: "North Carolina" },
  { code: "nj", label: "New Jersey" },
  { code: "ny", label: "New York" },
  { code: "oh", label: "Ohio" },
  { code: "pa", label: "Pennsylvania" },
  { code: "tn", label: "Tennessee" },
  { code: "va", label: "Virginia" },
  { code: "vt", label: "Vermont" },
  { code: "wv", label: "West Virginia" },
] as const;

export const useUserSettings = create<UserSettings>((set, get) => ({
  fdState: "nj",
  _hydrated: false,

  setFdState: (state: string) => {
    const s = state.toLowerCase().trim();
    set({ fdState: s });
    AsyncStorage.setItem(KEY, JSON.stringify({ fdState: s }));
  },

  hydrate: async () => {
    if (get()._hydrated) return;
    try {
      const raw = await AsyncStorage.getItem(KEY);
      if (raw) {
        const parsed = JSON.parse(raw);
        if (parsed.fdState) set({ fdState: parsed.fdState });
      }
    } catch {}
    set({ _hydrated: true });
  },
}));

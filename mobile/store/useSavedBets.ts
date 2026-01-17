import { create } from "zustand";
import AsyncStorage from "@react-native-async-storage/async-storage";

const STORAGE_KEY = "saved_props_v1";
const BETS_STORAGE_KEY = "saved_props_bets_v1"; // 🆕 ADD

// 🆕 ADD: richer bet model
export type SavedBet = {
  id: string;
  gameId: number;
  playerId: number;
  player: string;
  market: string;
  line: number;
  side: "over" | "under" | "milestone";
  odds?: number;
};

type SavedBetsStore = {
  savedIds: Set<string>;
  bets: Map<string, SavedBet>;
  toggleSave: (bet: SavedBet) => void;
  clearAll: () => void;
  hydrate: () => Promise<void>;
};

export const useSavedBets = create<SavedBetsStore>((set, get) => ({
  // =========================
  // EXISTING STATE
  // =========================
  savedIds: new Set(),

  // =========================
  // 🆕 ADDITION
  // =========================
  bets: new Map(),

  // =========================
  // EXTENDED (NOT REPLACED)
  // =========================
  toggleSave: (bet) => {
    const nextIds = new Set(get().savedIds);
    const nextBets = new Map(get().bets);
  
    if (nextIds.has(bet.id)) {
      // REMOVE
      nextIds.delete(bet.id);
      nextBets.delete(bet.id);
    } else {
      // ADD
      nextIds.add(bet.id);
      nextBets.set(bet.id, bet);
    }
  
    set({
      savedIds: nextIds,
      bets: nextBets,
    });
  
    AsyncStorage.setItem(
      STORAGE_KEY,
      JSON.stringify(Array.from(nextIds))
    );
  
    AsyncStorage.setItem(
      BETS_STORAGE_KEY,
      JSON.stringify(Array.from(nextBets.values()))
    );
  },

    set({
      savedIds: nextIds,
      bets: nextBets,
    });

    // =========================
    // PERSIST (BACKWARD SAFE)
    // =========================
    AsyncStorage.setItem(
      STORAGE_KEY,
      JSON.stringify(Array.from(nextIds))
    );

    AsyncStorage.setItem(
      BETS_STORAGE_KEY,
      JSON.stringify(Array.from(nextBets.values()))
    );
  },

  // =========================
  // EXISTING (UNCHANGED)
  // =========================
  clearAll: () => {
    set({
      savedIds: new Set(),
      bets: new Map(),
    });

    AsyncStorage.removeItem(STORAGE_KEY);
    AsyncStorage.removeItem(BETS_STORAGE_KEY); // 🆕 ADD
  },

  // =========================
  // EXTENDED HYDRATION
  // =========================
  hydrate: async () => {
    // ---- legacy IDs ----
    const raw = await AsyncStorage.getItem(STORAGE_KEY);
    if (raw) {
      set({ savedIds: new Set(JSON.parse(raw)) });
    }

    // ---- 🆕 bet objects ----
    const rawBets = await AsyncStorage.getItem(BETS_STORAGE_KEY);
    if (rawBets) {
      const parsed: SavedBet[] = JSON.parse(rawBets);
      const map = new Map<string, SavedBet>();
      parsed.forEach((b) => map.set(b.id, b));
      set({ bets: map });
    }
  },
}));
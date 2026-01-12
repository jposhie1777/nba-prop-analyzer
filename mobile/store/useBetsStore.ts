// store/useBetsStore.ts
import { create } from "zustand";
import { MarketSelection } from "@/types/betting";

export type SavedBet = {
  betId: string;          // same as selectionId (for now)
  selectionId: string;

  gameId: string;
  playerId?: string;
  playerName?: string;
  marketKey: string;
  outcome: string;
  line: number;

  odds: number;
  book: string;

  addedAt: number;
};

type BetsStore = {
  betsById: Record<string, SavedBet>;

  isSelected: (selectionId: string) => boolean;

  toggleBet: (selection: MarketSelection) => void;

  removeBet: (selectionId: string) => void;

  clearAll: () => void;
};

export const useBetsStore = create<BetsStore>((set, get) => ({
  betsById: {},

  isSelected(selectionId) {
    return Boolean(get().betsById[selectionId]);
  },

  toggleBet(selection) {
    const { selectionId } = selection;
    const existing = get().betsById[selectionId];

    if (existing) {
      // ❌ REMOVE
      set((state) => {
        const next = { ...state.betsById };
        delete next[selectionId];
        return { betsById: next };
      });
      return;
    }

    // ✅ ADD
    set((state) => ({
      betsById: {
        ...state.betsById,
        [selectionId]: {
          betId: selectionId,
          selectionId,

          gameId: selection.gameId,
          playerId: selection.playerId,

          marketKey: selection.marketKey,
          outcome: selection.outcome,
          line: selection.line,

          odds: selection.best.odds,
          book: selection.best.book,

          addedAt: Date.now(),
        },
      },
    }));
  },

  removeBet(selectionId) {
    set((state) => {
      const next = { ...state.betsById };
      delete next[selectionId];
      return { betsById: next };
    });
  },

  clearAll() {
    set({ betsById: {} });
  },
}));
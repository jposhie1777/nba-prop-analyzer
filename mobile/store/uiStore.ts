//.  /store/uiStore
import { create } from "zustand";
import { GameId } from "@/types/betting";

type UIStore = {
  expandedGames: Set<GameId>;
  toggleGame: (id: GameId) => void;
  isExpanded: (id: GameId) => boolean;
};

export const useUIStore = create<UIStore>((set, get) => ({
  expandedGames: new Set(),

  toggleGame(id) {
    set((state) => {
      const next = new Set(state.expandedGames);
      next.has(id) ? next.delete(id) : next.add(id);
      return { expandedGames: next };
    });
  },

  isExpanded(id) {
    return get().expandedGames.has(id);
  },
}));
// store/useBetslip.ts
import { create } from "zustand";
import { Bet } from "@/types/bet";

type BetslipState = {
  bets: Bet[];
  addBet: (bet: Bet) => void;
  removeBet: (id: string) => void;
  clear: () => void;
};

export const useBetslip = create<BetslipState>((set, get) => ({
  bets: [],

  addBet: (bet) => {
    const exists = get().bets.some(b => b.id === bet.id);
    if (exists) return;

    set(state => ({
      bets: [...state.bets, bet],
    }));
  },

  removeBet: (id) =>
    set(state => ({
      bets: state.bets.filter(b => b.id !== id),
    })),

  clear: () => set({ bets: [] }),
}));
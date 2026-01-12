// lib/auth/useAuth.ts
import * as SecureStore from "expo-secure-store";
import { create } from "zustand";

type AuthState = {
  accessToken?: string;
  role?: "dev" | "user";
  setAuth: (token: string, role: "dev" | "user") => void;
  logout: () => void;
};

export const useAuth = create<AuthState>((set) => ({
  accessToken: undefined,
  role: undefined,

  setAuth: (token, role) => {
    SecureStore.setItemAsync("access_token", token);
    set({ accessToken: token, role });
  },

  logout: () => {
    SecureStore.deleteItemAsync("access_token");
    set({ accessToken: undefined, role: undefined });
  },
}));

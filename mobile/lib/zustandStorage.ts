// lib/zustandStorage.ts
import { Platform } from "react-native";
import type { StateStorage } from "zustand/middleware";
import AsyncStorage from "@react-native-async-storage/async-storage";

/**
 * No-op storage used during SSR to prevent crashes.
 */
const noopStorage: StateStorage = {
  getItem: async () => null,
  setItem: async () => {},
  removeItem: async () => {},
};

export const createSafeStorage = (): StateStorage => {
  // 🚫 Web SSR (no window)
  if (Platform.OS === "web" && typeof window === "undefined") {
    return noopStorage;
  }

  // ✅ Web client → localStorage
  if (Platform.OS === "web") {
    return {
      getItem: async (name) => {
        try {
          return window.localStorage.getItem(name);
        } catch {
          return null;
        }
      },
      setItem: async (name, value) => {
        try {
          window.localStorage.setItem(name, value);
        } catch {}
      },
      removeItem: async (name) => {
        try {
          window.localStorage.removeItem(name);
        } catch {}
      },
    };
  }

  // ✅ Native (iOS / Android)
  return AsyncStorage;
};
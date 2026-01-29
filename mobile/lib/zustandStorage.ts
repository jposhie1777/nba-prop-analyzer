// lib/zustandStorage.ts
import { Platform } from "react-native";
import type { StateStorage } from "zustand/middleware";

/**
 * No-op storage used during SSR to prevent crashes.
 */
const noopStorage: StateStorage = {
  getItem: async () => null,
  setItem: async () => {},
  removeItem: async () => {},
};

export const createSafeStorage = (): StateStorage => {
  // 🚫 Web SSR (no window, no storage)
  if (Platform.OS === "web" && typeof window === "undefined") {
    return noopStorage;
  }

  // ✅ Native + Web client
  return {
    getItem: async (name) => {
      const AsyncStorage = await import(
        "@react-native-async-storage/async-storage"
      );
      return AsyncStorage.default.getItem(name);
    },
    setItem: async (name, value) => {
      const AsyncStorage = await import(
        "@react-native-async-storage/async-storage"
      );
      return AsyncStorage.default.setItem(name, value);
    },
    removeItem: async (name) => {
      const AsyncStorage = await import(
        "@react-native-async-storage/async-storage"
      );
      return AsyncStorage.default.removeItem(name);
    },
  };
};

// lib/zustandStorage.ts
import { Platform } from "react-native";
import type { StateStorage } from "zustand/middleware";
import AsyncStorage from "@react-native-async-storage/async-storage";

/**
 * No-op storage used during SSR to prevent crashes.
 */
const noopStorage: StateStorage = {
  getItem: () => null,
  setItem: () => {},
  removeItem: () => {},
};

const canUseWebStorage = () => {
  try {
    return typeof window !== "undefined" && !!window.localStorage;
  } catch {
    return false;
  }
};

const createWebStorage = (): StateStorage => ({
  getItem: (name) => {
    try {
      return window.localStorage.getItem(name);
    } catch {
      return null;
    }
  },
  setItem: (name, value) => {
    try {
      window.localStorage.setItem(name, value);
    } catch {}
  },
  removeItem: (name) => {
    try {
      window.localStorage.removeItem(name);
    } catch {}
  },
});

export const createSafeStorage = (): StateStorage => {
  // ✅ Web client → localStorage (even if Platform is mis-detected)
  if (canUseWebStorage()) {
    return createWebStorage();
  }

  // 🚫 Web SSR (no window)
  if (Platform.OS === "web") {
    return noopStorage;
  }

  // ✅ Native (iOS / Android)
  return AsyncStorage;
};
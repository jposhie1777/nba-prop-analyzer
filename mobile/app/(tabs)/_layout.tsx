// app/(tabs)/_layout.tsx
import React, { useEffect, useState, useCallback } from "react";
import { Tabs, Redirect } from "expo-router";
import AsyncStorage from "@react-native-async-storage/async-storage";

import { useAuth } from "@/lib/auth/useAuth";
import { HapticTab } from "@/components/haptic-tab";
import { IconSymbol } from "@/components/ui/icon-symbol";
import { Colors } from "@/constants/theme";
import { useColorScheme } from "@/hooks/use-color-scheme";

const SAVED_PROPS_KEY = "saved_props_v1";

export default function TabLayout() {
  const colorScheme = useColorScheme();

  const accessToken = true;
  const role = useAuth((s) => s.role);

  /* -------------------------------------------------
     🔐 PASSIVE AUTH GATE (SAFE)
     - No login()
     - No effects
     - No loops
  -------------------------------------------------- */
  if (!accessToken) {
    return <Redirect href="/login" />;
  }

  /* -------------------------------------------------
     SAVED BETS STATE
  -------------------------------------------------- */
  const [savedIds, setSavedIds] = useState<Set<string>>(new Set());

  // Load saved bets once
  useEffect(() => {
    AsyncStorage.getItem(SAVED_PROPS_KEY).then((raw) => {
      if (!raw) return;
      try {
        setSavedIds(new Set(JSON.parse(raw)));
      } catch {
        setSavedIds(new Set());
      }
    });
  }, []);

  // Persist saved bets
  useEffect(() => {
    AsyncStorage.setItem(
      SAVED_PROPS_KEY,
      JSON.stringify(Array.from(savedIds))
    );
  }, [savedIds]);

  /* -------------------------------------------------
     HELPERS (MEMOIZED)
  -------------------------------------------------- */
  const toggleSave = useCallback((id: string) => {
    setSavedIds((prev) => {
      const next = new Set(prev);
      next.has(id) ? next.delete(id) : next.add(id);
      return next;
    });
  }, []);

  const clearAllSaved = useCallback(() => {
    setSavedIds(new Set());
  }, []);

  /* -------------------------------------------------
     TABS
  -------------------------------------------------- */
  return (
    <Tabs
      screenOptions={{
        headerShown: false,
        tabBarButton: HapticTab,
        tabBarActiveTintColor: Colors[colorScheme ?? "light"].tint,
      }}
    >
      {/* HOME */}
      <Tabs.Screen
        name="index"
        options={{
          title: "Home",
          tabBarIcon: ({ color }) => (
            <IconSymbol size={28} name="house.fill" color={color} />
          ),
        }}
        initialParams={{
          savedIds,
          toggleSave,
        }}
      />

      {/* LIVE */}
      <Tabs.Screen
        name="live"
        options={{
          title: "Live",
          tabBarIcon: ({ color }) => (
            <IconSymbol
              size={28}
              name="dot.radiowaves.left.and.right"
              color={color}
            />
          ),
        }}
      />

      {/* SAVED */}
      <Tabs.Screen
        name="saved"
        options={{
          title: "Saved",
          tabBarIcon: ({ color }) => (
            <IconSymbol size={28} name="bookmark.fill" color={color} />
          ),
        }}
        initialParams={{
          savedIds,
          toggleSave,
          clearAllSaved,
        }}
      />

      {/* DEV (ROLE-GATED) */}
      {role === "dev" && (
        <Tabs.Screen
          name="dev"
          options={{
            title: "Dev",
            tabBarIcon: ({ color }) => (
              <IconSymbol
                size={28}
                name="wrench.and.screwdriver.fill"
                color={color}
              />
            ),
          }}
        />
      )}
    </Tabs>
  );
}
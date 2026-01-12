//   /app/(tabs)/_layout.tsx
import { Tabs } from "expo-router";
import React, { useEffect, useState } from "react";
import AsyncStorage from "@react-native-async-storage/async-storage";
import { useAuth } from "@/lib/auth/useAuth";

import { HapticTab } from "@/components/haptic-tab";
import { IconSymbol } from "@/components/ui/icon-symbol";
import { Colors } from "@/constants/theme";
import { useColorScheme } from "@/hooks/use-color-scheme";

const SAVED_PROPS_KEY = "saved_props_v1";

export default function TabLayout() {
  const colorScheme = useColorScheme();
  const role = useAuth((s) => s.role);

  // ---------------------------
  // GLOBAL SAVED BETS STATE
  // ---------------------------
  const [savedIds, setSavedIds] = useState<Set<string>>(new Set());

  // ---------------------------
  // LOAD SAVED BETS
  // ---------------------------
  useEffect(() => {
    AsyncStorage.getItem(SAVED_PROPS_KEY).then((raw) => {
      if (!raw) return;
      setSavedIds(new Set(JSON.parse(raw)));
    });
  }, []);

  // ---------------------------
  // PERSIST SAVED BETS
  // ---------------------------
  useEffect(() => {
    AsyncStorage.setItem(
      SAVED_PROPS_KEY,
      JSON.stringify(Array.from(savedIds))
    );
  }, [savedIds]);

  // ---------------------------
  // HELPERS
  // ---------------------------
  const toggleSave = (id: string) => {
    setSavedIds((prev) => {
      const next = new Set(prev);
      next.has(id) ? next.delete(id) : next.add(id);
      return next;
    });
  };

  const clearAllSaved = () => {
    setSavedIds(new Set());
  };

  return (
    <Tabs
      screenOptions={{
        tabBarActiveTintColor: Colors[colorScheme ?? "light"].tint,
        headerShown: false,
        tabBarButton: HapticTab,
      }}
    >
      {/* HOME TAB */}
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
      {/* LIVE TAB */}
      <Tabs.Screen
        name="live"
        options={{
          title: "Live",
          tabBarIcon: ({ color }) => (
            <IconSymbol size={28} name="dot.radiowaves.left.and.right" color={color} />
          ),
        }}
      />

      {/* SAVED TAB */}
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
      {/* DEV TAB (DEV ONLY) */}
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
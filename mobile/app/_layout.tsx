// app/_layout.tsx
import { useEffect, useState } from "react";
import { Platform } from "react-native";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import {
  DarkTheme,
  DefaultTheme,
  ThemeProvider,
} from "@react-navigation/native";
import { Slot } from "expo-router";
import { StatusBar } from "expo-status-bar";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";

import { useColorScheme } from "@/hooks/use-color-scheme";
import { useSavedProps } from "@/store/useSavedProps";
import { useDevStore } from "@/lib/dev/devStore";
import { PropBetslipDrawer } from "@/components/prop/PropBetslipDrawer";
import { ensurePushRegistered } from "@/lib/notifications/registerForPush";

export const unstable_settings = {
  anchor: "(tabs)",
};

export default function RootLayout() {
  console.log("🎨 RootLayout render");

  const colorScheme = useColorScheme();
  const [queryClient] = useState(() => new QueryClient());

  const hydrateSavedProps = useSavedProps((s) => s.hydrate);

  useEffect(() => {
    hydrateSavedProps();
  }, [hydrateSavedProps]);

  useEffect(() => {
    if (__DEV__) {
      useDevStore.getState().actions.hydrateFlags();
    }
  }, []);

  useEffect(() => {
    if (Platform.OS !== "web") {
      ensurePushRegistered("anon").catch(() => {});
    }
  }, []);

  return (
    <GestureHandlerRootView style={{ flex: 1 }}>
      <QueryClientProvider client={queryClient}>
        <ThemeProvider
          value={colorScheme === "dark" ? DarkTheme : DefaultTheme}
        >
          {/* 🔑 THIS IS THE FIX */}
          <Slot />

          <PropBetslipDrawer />
          <StatusBar style="auto" />
        </ThemeProvider>
      </QueryClientProvider>
    </GestureHandlerRootView>
  );
}
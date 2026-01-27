// app/_layout.tsx
import "react-native-reanimated";
import { useEffect, useState } from "react";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import {
  DarkTheme,
  DefaultTheme,
  ThemeProvider,
} from "@react-navigation/native";
import { Stack } from "expo-router";
import { StatusBar } from "expo-status-bar";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";

import { useColorScheme } from "@/hooks/use-color-scheme";
import { useSavedProps } from "@/store/useSavedProps";
import { useDevStore } from "@/lib/dev/devStore";
import { installFetchInterceptor } from "@/lib/dev/interceptFetch";

// ✅ ADD THIS
import PropBetslipDrawer from "@/components/prop/PropBetslipDrawer";

/* -------------------------------------------------
   Expo Router settings
-------------------------------------------------- */
export const unstable_settings = {
  anchor: "(tabs)",
};

/* -------------------------------------------------
   GLOBAL DEV INSTRUMENTATION (DEV ONLY)
-------------------------------------------------- */
if (__DEV__) {
  // @ts-ignore
  const defaultHandler = global.ErrorUtils?.getGlobalHandler?.();

  // @ts-ignore
  global.ErrorUtils?.setGlobalHandler?.(
    (error: Error, isFatal?: boolean) => {
      try {
        useDevStore.getState().actions.logError(error);
      } catch {}

      if (defaultHandler) {
        defaultHandler(error, isFatal);
      }
    }
  );

  installFetchInterceptor();
}

/* -------------------------------------------------
   ROOT LAYOUT
-------------------------------------------------- */
export default function RootLayout() {
  const colorScheme = useColorScheme();
  const [queryClient] = useState(() => new QueryClient());

  // Hydrate saved props on boot
  const hydrateSavedProps = useSavedProps((s) => s.hydrate);

  useEffect(() => {
    hydrateSavedProps();
  }, [hydrateSavedProps]);

  // Hydrate dev flags
  useEffect(() => {
    if (__DEV__) {
      useDevStore.getState().actions.hydrateFlags();
    }
  }, []);

  return (
    <GestureHandlerRootView style={{ flex: 1 }}>
      <QueryClientProvider client={queryClient}>
        <ThemeProvider
          value={colorScheme === "dark" ? DarkTheme : DefaultTheme}
        >
          {/* -------------------------------
              APP NAVIGATION
          -------------------------------- */}
          <Stack screenOptions={{ headerShown: false }}>
            <Stack.Screen name="(tabs)" />
          </Stack>

          {/* -------------------------------
              GLOBAL OVERLAYS
          -------------------------------- */}
          <PropBetslipDrawer />

          <StatusBar style="auto" />
        </ThemeProvider>
      </QueryClientProvider>
    </GestureHandlerRootView>
  );
}
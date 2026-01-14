// tabs/layout
import "react-native-reanimated";
import { useEffect } from "react";
import {
  DarkTheme,
  DefaultTheme,
  ThemeProvider,
} from "@react-navigation/native";
import { Stack } from "expo-router";
import { StatusBar } from "expo-status-bar";

import { useColorScheme } from "@/hooks/use-color-scheme";
import { useSavedBets } from "@/store/useSavedBets";
import DebugMemory from "@/components/debug/DebugMemory";
import { useDevStore } from "@/lib/dev/devStore";
import { installFetchInterceptor } from "@/lib/dev/interceptFetch";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { useState } from "react";

/* -------------------------------------------------
   Expo Router settings
-------------------------------------------------- */
export const unstable_settings = {
  anchor: "(tabs)",
};

/* -------------------------------------------------
   GLOBAL DEV INSTRUMENTATION (DEV ONLY)
   - Registered once
   - Memory safe
   - Preserves RedBox
-------------------------------------------------- */
if (__DEV__) {
  // -----------------------------
  // Global error capture
  // -----------------------------
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

  // -----------------------------
  // Fetch interceptor
  // -----------------------------
  installFetchInterceptor();
}

/* -------------------------------------------------
   ROOT LAYOUT
-------------------------------------------------- */
export default function RootLayout() {
  const colorScheme = useColorScheme();
  const [queryClient] = useState(() => new QueryClient());
  // ---------------------------
  // HYDRATE SAVED BETS ON BOOT
  // ---------------------------
  const hydrateSavedBets = useSavedBets((s) => s.hydrate);

  useEffect(() => {
    hydrateSavedBets();
  }, [hydrateSavedBets]);

  // ---------------------------
  // 🔴 NEW: HYDRATE DEV FLAGS ON BOOT (4A)
  // ---------------------------
  useEffect(() => {
    if (__DEV__) {
      useDevStore.getState().actions.hydrateFlags();
    }
  }, []);

  return (
    <QueryClientProvider client={queryClient}>
      <>
        {/* DEV-ONLY MEMORY OVERLAY */}
        {__DEV__ && <DebugMemory />}
  
        <ThemeProvider
          value={colorScheme === "dark" ? DarkTheme : DefaultTheme}
        >
          <Stack>
            {/* MAIN TAB STACK */}
            <Stack.Screen name="(tabs)" options={{ headerShown: false }} />
  
            {/* MODALS */}
            <Stack.Screen
              name="modal"
              options={{ presentation: "modal", title: "Modal" }}
            />
          </Stack>
  
          <StatusBar style="auto" />
        </ThemeProvider>
      </>
    </QueryClientProvider>
  );
}
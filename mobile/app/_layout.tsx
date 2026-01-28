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
import { PropBetslipDrawer } from "@/components/prop/PropBetslipDrawer";

import { registerForPushNotifications } from "@/lib/notifications/registerForPush";
import { API_BASE } from "@/lib/apiMaster";

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

  /* -------------------------------
     HYDRATE STORES ON BOOT
  -------------------------------- */
  const hydrateSavedProps = useSavedProps((s) => s.hydrate);

  useEffect(() => {
    hydrateSavedProps();
  }, [hydrateSavedProps]);

  useEffect(() => {
    if (__DEV__) {
      useDevStore.getState().actions.hydrateFlags();
    }
  }, []);

  /* -------------------------------
     REGISTER PUSH NOTIFICATIONS
     (RUNS ONCE PER APP LAUNCH)
  -------------------------------- */
  useEffect(() => {
    (async () => {
      try {
        const token = await registerForPushNotifications();
        if (!token) return;

        await fetch(`${API_BASE}/push/register`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            expo_push_token: token,
            user_id: "anon", // replace when auth exists
          }),
        });
      } catch (err) {
        console.warn("Push registration failed", err);
      }
    })();
  }, []);

  /* -------------------------------
     RENDER
  -------------------------------- */
  return (
    <GestureHandlerRootView style={{ flex: 1 }}>
      <QueryClientProvider client={queryClient}>
        <ThemeProvider
          value={colorScheme === "dark" ? DarkTheme : DefaultTheme}
        >
          {/* APP NAVIGATION */}
          <Stack screenOptions={{ headerShown: false }}>
            <Stack.Screen name="(tabs)" />
          </Stack>

          {/* GLOBAL OVERLAYS */}
          <PropBetslipDrawer />

          <StatusBar style="auto" />
        </ThemeProvider>
      </QueryClientProvider>
    </GestureHandlerRootView>
  );
}
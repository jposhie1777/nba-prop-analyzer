Pasting for reference 

// app/_layout.tsx
import "react-native-reanimated";
import { useEffect, useState } from "react";
import { Platform, View, Text } from "react-native";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import {
  DarkTheme,
  DefaultTheme,
  ThemeProvider,
} from "@react-navigation/native";
import { Stack, usePathname } from "expo-router";
import { StatusBar } from "expo-status-bar";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";

import { useColorScheme } from "@/hooks/use-color-scheme";
import { useSavedProps } from "@/store/useSavedProps";
import { useDevStore } from "@/lib/dev/devStore";
import { installFetchInterceptor } from "@/lib/dev/interceptFetch";
import { PropBetslipDrawer } from "@/components/prop/PropBetslipDrawer";
import { ensurePushRegistered } from "@/lib/notifications/registerForPush";

/* -------------------------------------------------
   Expo Router settings
-------------------------------------------------- */
export const unstable_settings = {
  anchor: "(tabs)",
};

/* -------------------------------------------------
   GLOBAL DEV INSTRUMENTATION (SAFE)
-------------------------------------------------- */
if (__DEV__) {
  console.log("🧪 DEV MODE ENABLED");

  // Catch render-time fatal errors
  // @ts-ignore
  const defaultHandler = global.ErrorUtils?.getGlobalHandler?.();

  // @ts-ignore
  global.ErrorUtils?.setGlobalHandler?.((error: Error, isFatal?: boolean) => {
    console.error("🔥 GLOBAL ERROR:", error);

    try {
      useDevStore.getState().actions.logError(error);
    } catch {}

    if (defaultHandler) {
      defaultHandler(error, isFatal);
    }
  });

  // ❗ WEB-SAFE GUARD
  if (Platform.OS !== "web") {
    console.log("🧪 Installing fetch interceptor (native only)");
    installFetchInterceptor();
  } else {
    console.log("⚠️ Skipping fetch interceptor on web");
  }
}

/* -------------------------------------------------
   ROOT LAYOUT
-------------------------------------------------- */
export default function RootLayout() {
  const colorScheme = useColorScheme();
  const pathname = usePathname();
  const [queryClient] = useState(() => new QueryClient());

  /* -------------------------------
     ROUTE DEBUG
  -------------------------------- */
  useEffect(() => {
    console.log("🧭 ROUTE CHANGED:", pathname);
  }, [pathname]);

  /* -------------------------------
     HYDRATE STORES
  -------------------------------- */
  const hydrateSavedProps = useSavedProps((s) => s.hydrate);

  useEffect(() => {
    console.log("💧 Hydrating saved props");
    hydrateSavedProps();
  }, [hydrateSavedProps]);

  useEffect(() => {
    if (__DEV__) {
      console.log("🧪 Hydrating dev flags");
      useDevStore.getState().actions.hydrateFlags();
    }
  }, []);

  /* -------------------------------
     PUSH REGISTRATION (SAFE)
  -------------------------------- */
  useEffect(() => {
    if (Platform.OS === "web") {
      console.log("⚠️ Skipping push registration on web");
      return;
    }

    ensurePushRegistered("anon").catch((err) => {
      console.warn("📵 Push registration skipped:", err);
    });
  }, []);

  /* -------------------------------
     HARD VISUAL DEBUG
  -------------------------------- */
  console.log("🎨 RootLayout render");

  return (
    <GestureHandlerRootView style={{ flex: 1, backgroundColor: "#111" }}>
      <QueryClientProvider client={queryClient}>
        <ThemeProvider
          value={colorScheme === "dark" ? DarkTheme : DefaultTheme}
        >
          {/* 🧭 APP NAVIGATION */}
          <Stack screenOptions={{ headerShown: false }}>
            <Stack.Screen name="(tabs)" />
          </Stack>

          {/* 🧪 GLOBAL OVERLAYS */}
          <PropBetslipDrawer />

          <StatusBar style="auto" />
        </ThemeProvider>
      </QueryClientProvider>
    </GestureHandlerRootView>
  );
}
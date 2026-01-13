// app/_layout.tsx
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
  global.ErrorUtils?.setGlobalHandler?.((error, isFatal) => {
    try {
      useDevStore.getState().actions.logError(error);
    } catch {}

    defaultHandler?.(error, isFatal);
  });

  installFetchInterceptor();
}

/* -------------------------------------------------
   ROOT LAYOUT (NO AUTH)
-------------------------------------------------- */
export default function RootLayout() {
  const colorScheme = useColorScheme();
  const hydrateSavedBets = useSavedBets((s) => s.hydrate);

  useEffect(() => {
    hydrateSavedBets();
  }, [hydrateSavedBets]);

  useEffect(() => {
    if (__DEV__) {
      useDevStore.getState().actions.hydrateFlags();
    }
  }, []);

  return (
    <>
      {__DEV__ && <DebugMemory />}

      <ThemeProvider
        value={colorScheme === "dark" ? DarkTheme : DefaultTheme}
      >
        <Stack>
          <Stack.Screen name="(tabs)" options={{ headerShown: false }} />
          <Stack.Screen name="login" options={{ headerShown: false }} />
          <Stack.Screen
            name="modal"
            options={{ presentation: "modal" }}
          />
        </Stack>

        <StatusBar style="auto" />
      </ThemeProvider>
    </>
  );
}
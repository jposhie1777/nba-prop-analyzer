import { DarkTheme, DefaultTheme, ThemeProvider } from "@react-navigation/native";
import { Stack } from "expo-router";
import { StatusBar } from "expo-status-bar";
import { useEffect } from "react";
import "react-native-reanimated";

import { useColorScheme } from "@/hooks/use-color-scheme";
import { useSavedBets } from "@/store/useSavedBets";

export const unstable_settings = {
  anchor: "(tabs)",
};

export default function RootLayout() {
  const colorScheme = useColorScheme();

  // ---------------------------
  // HYDRATE SAVED BETS ON BOOT
  // ---------------------------
  const hydrateSavedBets = useSavedBets((s) => s.hydrate);

  useEffect(() => {
    hydrateSavedBets();
  }, [hydrateSavedBets]);

  return (
    <ThemeProvider value={colorScheme === "dark" ? DarkTheme : DefaultTheme}>
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
  );
}
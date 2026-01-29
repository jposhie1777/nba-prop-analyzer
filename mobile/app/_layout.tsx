// app/_layout.tsx
import { View } from "react-native";
import { Stack } from "expo-router";
import { RouterDebug } from "@/lib/dev/RouterDebug";

export default function RootLayout() {
  console.log("🟥 ROOT LAYOUT RENDER");

  return (
    <View style={{ flex: 1 }}>
      {/* 🧪 Router Debug Overlay */}
      {__DEV__ && <RouterDebug />}

      {/* 🧭 App Navigation */}
      <Stack
        screenOptions={{ headerShown: false }}
        initialRouteName="(tabs)"
      >
        <Stack.Screen name="(tabs)" />
      </Stack>
    </View>
  );
}
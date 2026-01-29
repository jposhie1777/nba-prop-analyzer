// app/_layout.tsx
import { Stack } from "expo-router";

export default function RootLayout() {
  console.log("🟥 ROOT LAYOUT RENDER");

  return (
    <Stack screenOptions={{ headerShown: false }}>
      <Stack.Screen name="(tabs)" />
    </Stack>
  );
}
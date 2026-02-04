// app/(tabs)/atp/_layout.tsx
import { Stack } from "expo-router";

export default function AtpLayout() {
  return (
    <Stack
      screenOptions={{
        headerShown: true,
        presentation: "card",
      }}
    />
  );
}

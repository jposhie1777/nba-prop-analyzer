// app/(tabs)/pga/_layout.tsx
import { Stack } from "expo-router";

export default function PgaLayout() {
  return (
    <Stack
      screenOptions={{
        headerShown: true,
        presentation: "card",
      }}
    />
  );
}

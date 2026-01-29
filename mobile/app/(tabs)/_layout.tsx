// app/(tabs)/_layout.tsx
// app/(tabs)/_layout.tsx
import { Tabs } from "expo-router";

export default function TabLayout() {
  console.log("🟢 TabLayout render");

  return (
    <Tabs>
      <Tabs.Screen
        name="home"
        options={{ title: "Home" }}
      />
    </Tabs>
  );
}
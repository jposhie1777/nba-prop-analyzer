// app/(tabs)/_layout.tsx
import { Tabs } from "expo-router";
import { Text } from "react-native";

export default function TabsLayout() {
  console.log("🟢 TABS LAYOUT RENDER");

  return (
    <Tabs>
      <Tabs.Screen name="home" />
      <Tabs.Screen name="props" />
      <Tabs.Screen name="live" />
      <Tabs.Screen name="first-basket" />
      <Tabs.Screen name="trend-chart" />
    </Tabs>
  );
}
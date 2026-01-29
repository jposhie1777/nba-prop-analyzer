// app/(tabs)/_layout.tsx
import { Tabs } from "expo-router";
import { Text } from "react-native";

export default function TabLayout() {
  console.log("🟢 TABS LAYOUT RENDER");

  return (
    <Tabs>
      <Tabs.Screen
        name="home"
        options={{
          title: "Home",
          tabBarIcon: () => <Text>🏠</Text>,
        }}
      />
    </Tabs>
  );
}
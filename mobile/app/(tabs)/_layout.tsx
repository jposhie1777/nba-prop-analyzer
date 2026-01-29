// app/(tabs)/_layout.tsx
import { Tabs } from "expo-router";
import { Text, View } from "react-native";

export default function TabLayout() {
  console.log("🧭 TabLayout render");

  return (
    <View style={{ flex: 1, backgroundColor: "red" }}>
      <Text style={{ color: "white", marginTop: 50 }}>
        TABS LAYOUT VISIBLE
      </Text>

      <Tabs>
        <Tabs.Screen name="home" />
      </Tabs>
    </View>
  );
}}
// app/_layout.tsx
import { View, Text } from "react-native";
import { Slot } from "expo-router";

export default function RootLayout() {
  console.log("🟥 ROOT + SLOT render");

  return (
    <View style={{ flex: 1, backgroundColor: "red" }}>
      {/* If routing works, you should see your actual screens render here */}
      <Slot />

      {/* If Slot renders nothing, you will still see this */}
      <View style={{ position: "absolute", bottom: 30, left: 20 }}>
        <Text style={{ color: "white", fontSize: 18 }}>ROOT + SLOT OK</Text>
      </View>
    </View>
  );
}
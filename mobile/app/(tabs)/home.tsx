// app/(tabs)/home.tsx
import { View, Text } from "react-native";

export default function Home() {
  console.log("🏠 HOME RENDER");

  return (
    <View
      style={{
        flex: 1,
        alignItems: "center",
        justifyContent: "center",
      }}
    >
      <Text>HOME OK</Text>
    </View>
  );
}
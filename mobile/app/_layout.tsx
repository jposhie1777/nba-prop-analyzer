// app/_layout.tsx
import { View, Text } from "react-native";

export default function RootLayout() {
  console.log("🚨 ROOT LAYOUT OVERRIDE RENDER");

  return (
    <View
      style={{
        flex: 1,
        backgroundColor: "red",
        alignItems: "center",
        justifyContent: "center",
      }}
    >
      <Text style={{ color: "white", fontSize: 24 }}>
        ROOT LAYOUT OK
      </Text>
    </View>
  );
}
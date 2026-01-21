// components/live/OddsButton.tsx
import { TouchableOpacity, Text } from "react-native";

export function OddsButton({ bet }: { bet: any }) {
  return (
    <TouchableOpacity
      onPress={() => {
        console.log("🟢 ODDS BUTTON PRESSED", bet?.display?.title);
      }}
      style={{
        paddingVertical: 12,
        paddingHorizontal: 16,
        borderRadius: 10,
        borderWidth: 2,
        borderColor: "red",
        backgroundColor: "rgba(255,0,0,0.1)",
      }}
    >
      <Text style={{ fontWeight: "700" }}>
        TEST BUTTON
      </Text>
    </TouchableOpacity>
  );
}
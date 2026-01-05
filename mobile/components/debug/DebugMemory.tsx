import { View, Text, StyleSheet } from "react-native";
import * as Device from "expo-device";

export default function DebugMemory() {
  if (!__DEV__) {
    console.log("[DebugMemory] skipped (not dev)");
    return null;
  }

  console.log("[DebugMemory] rendering");

  const deviceName = Device.modelName ?? "Unknown Device";
  const totalRamGb = Device.totalMemory
    ? Math.round(Device.totalMemory / 1e9)
    : null;

  return (
    <View style={styles.container} pointerEvents="none">
      <Text style={styles.text}>📱 {deviceName}</Text>
      <Text style={styles.text}>
        RAM: {totalRamGb !== null ? `${totalRamGb} GB` : "N/A"}
      </Text>
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    position: "absolute",
    bottom: 12,
    left: 12,
    backgroundColor: "rgba(0,0,0,0.75)",
    paddingHorizontal: 10,
    paddingVertical: 6,
    borderRadius: 8,
    zIndex: 9999,
  },
  text: {
    color: "#fff",
    fontSize: 11,
    lineHeight: 14,
  },
});

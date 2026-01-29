import { Text, View } from "react-native";
import { usePathname, useSegments } from "expo-router";

export function RouterDebug() {
  const pathname = usePathname();
  const segments = useSegments();

  return (
    <View
      style={{
        position: "absolute",
        top: 40,
        left: 10,
        zIndex: 9999,
        padding: 8,
        borderRadius: 8,
        backgroundColor: "rgba(0,0,0,0.7)",
      }}
    >
      <Text style={{ color: "white", fontSize: 12 }}>
        PATH: {pathname}
      </Text>
      <Text style={{ color: "white", fontSize: 12 }}>
        SEG: {segments.join("/")}
      </Text>
    </View>
  );
}
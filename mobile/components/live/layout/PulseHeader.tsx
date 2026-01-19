import { View, Text, Image, StyleSheet } from "react-native";
import { useTheme } from "@/store/useTheme";

export function PulseHeader() {
  const { colors } = useTheme();

  return (
    <View
      style={[
        styles.container,
        { backgroundColor: colors.background, borderBottomColor: colors.border },
      ]}
    >
      <Image
        source={require("@/assets/logo.png")} // put your logo here
        style={styles.logo}
        resizeMode="contain"
      />
      <Text style={[styles.title, { color: colors.text }]}>
        Pulse Sports Analytics
      </Text>
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    height: 56,
    paddingHorizontal: 16,
    flexDirection: "row",
    alignItems: "center",
    borderBottomWidth: StyleSheet.hairlineWidth,
  },
  logo: { width: 28, height: 28, marginRight: 10 },
  title: { fontSize: 18, fontWeight: "800" },
});

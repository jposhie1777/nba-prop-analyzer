import { View, Text, Image, StyleSheet } from "react-native";
import { useTheme } from "@/store/useTheme";

export function PulseHeader() {
  const { colors } = useTheme();

  return (
    <View style={[styles.container, { backgroundColor: colors.background }]}>
      <Image
        source={require("@/assets/logo.png")} // or uri
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
    height: 64,
    flexDirection: "row",
    alignItems: "center",
    paddingHorizontal: 16,
    borderBottomWidth: StyleSheet.hairlineWidth,
  },

  logo: {
    width: 40,
    height: 40,
    marginRight: 12,
  },

  title: {
    fontSize: 18,
    fontWeight: "700",
    letterSpacing: 0.3,
  },
});

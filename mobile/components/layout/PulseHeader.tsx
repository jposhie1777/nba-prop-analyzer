import { View, Text, Image, StyleSheet } from "react-native";
import { useTheme } from "@/store/useTheme";

export function PulseHeader() {
  const { colors } = useTheme();

  return (
    <View
      style={[
        styles.container,
        {
          backgroundColor: colors.surface.screen,
          borderBottomColor: colors.border.subtle,
        },
      ]}
    >
      <View
        style={[
          styles.logoWrap,
          { backgroundColor: colors.surface.card },
        ]}
      >
        <Image
          source={require("@/assets/logo.png")}
          style={styles.logo}
          resizeMode="contain"
        />
      </View>

      <Text
        style={[
          styles.title,
          { color: colors.text.primary },
        ]}
      >
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

  logoWrap: {
    width: 44,
    height: 44,
    borderRadius: 10,
    alignItems: "center",
    justifyContent: "center",
    marginRight: 12,
  },

  logo: {
    width: 32,
    height: 32,
  },

  title: {
    fontSize: 18,
    fontWeight: "700",
    letterSpacing: 0.3,
  },
});
import { View, Text, Image, StyleSheet, Pressable, Platform } from "react-native";
import { SafeAreaView } from "react-native-safe-area-context";
import { useTheme } from "@/store/useTheme";
import { useRouter } from "expo-router";
import { useDevStore } from "@/lib/dev/devStore";

export function PulseHeader() {
  const { colors } = useTheme();
  const router = useRouter();
  const registerDevTap = useDevStore((s) => s.actions.registerDevTap);

  function handleDevTap() {
    registerDevTap();
    if (useDevStore.getState().devUnlocked) {
      router.push("/(dev)/dev-home");
    }
  }

  return (
    <SafeAreaView
      edges={["top"]}
      style={{ backgroundColor: colors.surface.screen }}
    >
      <Pressable onPress={handleDevTap}>
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
              {
                backgroundColor: colors.accent.primary,
                ...Platform.select({
                  ios: {
                    shadowColor: colors.accent.primary,
                    shadowOffset: { width: 0, height: 2 },
                    shadowOpacity: 0.25,
                    shadowRadius: 6,
                  },
                  android: { elevation: 3 },
                  default: {},
                }),
              },
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
      </Pressable>
    </SafeAreaView>
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
    width: 38,
    height: 38,
    borderRadius: 10,
    alignItems: "center",
    justifyContent: "center",
    marginRight: 12,
  },

  logo: {
    width: 24,
    height: 24,
    tintColor: "#FFFFFF",
  },

  title: {
    fontSize: 17,
    fontWeight: "700",
    letterSpacing: -0.2,
  },
});

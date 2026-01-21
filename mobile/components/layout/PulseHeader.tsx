import { View, Text, Image, StyleSheet, Pressable } from "react-native";
import { SafeAreaView } from "react-native-safe-area-context";
import { useTheme } from "@/store/useTheme";
import { useRouter } from "expo-router";
import { useRef } from "react";

export function PulseHeader() {
  const { colors } = useTheme();
  const router = useRouter();

  // DEV-only 5-tap unlock
  const tapCountRef = useRef(0);
  const tapTimerRef = useRef<NodeJS.Timeout | null>(null);

  function handleDevTap() {
    if (!__DEV__) return;

    tapCountRef.current += 1;

    if (tapCountRef.current === 1) {
      tapTimerRef.current = setTimeout(() => {
        tapCountRef.current = 0;
        tapTimerRef.current = null;
      }, 2000);
    }

    if (tapCountRef.current >= 5) {
      tapCountRef.current = 0;
      if (tapTimerRef.current) {
        clearTimeout(tapTimerRef.current);
        tapTimerRef.current = null;
      }
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
          {/* LOGO — PRIMARY ANCHOR */}
          <View
            style={[
              styles.logoWrap,
              {
                backgroundColor: colors.surface.card,
                borderColor: colors.border.subtle,
              },
            ]}
          >
            <Image
              source={require("@/assets/logo.png")}
              style={styles.logo}
              resizeMode="contain"
            />
          </View>

          {/* TEXT — SECONDARY */}
          <View style={styles.textWrap}>
            <Text
              style={[
                styles.title,
                { color: colors.text.primary },
              ]}
            >
              Pulse
            </Text>
            <Text
              style={[
                styles.subtitle,
                { color: colors.text.muted },
              ]}
            >
              Sports Analytics
            </Text>
          </View>
        </View>
      </Pressable>
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  container: {
    height: 72, // ⬆️ more breathing room
    flexDirection: "row",
    alignItems: "center",
    paddingHorizontal: 16,
    borderBottomWidth: StyleSheet.hairlineWidth,
  },

  logoWrap: {
    width: 52,   // ⬆️ logo is now unmistakable
    height: 52,
    borderRadius: 14,
    alignItems: "center",
    justifyContent: "center",
    borderWidth: 1,
  },

  logo: {
    width: 40,
    height: 40,
  },

  textWrap: {
    marginLeft: 12,
    justifyContent: "center",
  },

  title: {
    fontSize: 18,
    fontWeight: "700",
    letterSpacing: 0.4,
    lineHeight: 20,
  },

  subtitle: {
    fontSize: 12,
    marginTop: -2,
    letterSpacing: 0.3,
  },
});
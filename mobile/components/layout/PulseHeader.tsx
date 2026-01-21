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
          {/* LOGO — PRIMARY BRAND ANCHOR */}
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

          {/* BRAND TEXT — SUPPORTING */}
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
    paddingTop: 10,
    paddingBottom: 12,
    alignItems: "center", // 🔑 forces logo-first hierarchy
    borderBottomWidth: StyleSheet.hairlineWidth,
  },

  logoWrap: {
    width: 64,          // 👈 unmistakable
    height: 64,
    borderRadius: 18,
    alignItems: "center",
    justifyContent: "center",
    borderWidth: 1,
    marginBottom: 6,
  },

  logo: {
    width: 48,
    height: 48,
  },

  title: {
    fontSize: 14,       // 👈 supporting, not competing
    fontWeight: "600",
    letterSpacing: 0.4,
  },
});
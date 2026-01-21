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
    width: 44,
    height: 44,
    borderRadius: 10,
    alignItems: "center",
    justifyContent: "center",
    marginRight: 12,
    borderWidth: 1,
    borderColor: "rgba(0,0,0,0.05)", // or colors.border.subtle
  },

  logo: {
    width: 36,
    height: 36,
  },

  title: {
    fontSize: 17,        // ⬇️ from 18
    fontWeight: "600",  // ⬇️ from 700
    letterSpacing: 0.2, // slightly tighter
  },
});
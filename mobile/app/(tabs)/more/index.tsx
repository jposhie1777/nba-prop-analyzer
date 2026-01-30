// app/(tabs)/more/index.tsx
import { View, Text, Pressable, StyleSheet } from "react-native";
import { useRouter } from "expo-router";

import { useTheme } from "@/store/useTheme";
import { useParlayTracker } from "@/store/useParlayTracker";

export default function MoreIndexScreen() {
  const { colors } = useTheme();
  const router = useRouter();

  const { tracked } = useParlayTracker();
  const trackedCount = Object.keys(tracked).length;

  return (
    <View
      style={[
        styles.container,
        { backgroundColor: colors.surface.base },
      ]}
    >
      {/* Tracked Parlays */}
      <Pressable
        onPress={() =>
          router.push("/(tabs)/more/tracked-parlays")
        }
        style={[
          styles.card,
          {
            backgroundColor: colors.surface.card,
            borderColor: colors.border.subtle,
          },
        ]}
      >
        <Text
          style={[
            styles.title,
            { color: colors.text.primary },
          ]}
        >
          📊 Tracked Parlays
          {trackedCount > 0 ? ` (${trackedCount})` : ""}
        </Text>

        <Text
          style={[
            styles.subtitle,
            { color: colors.text.muted },
          ]}
        >
          View and manage tracked bets
        </Text>
      </Pressable>

      {/* Live Props (DEV) */}
      <Pressable
        onPress={() => router.push("/live-props-dev")}
        style={[
          styles.card,
          {
            backgroundColor: colors.surface.card,
            borderColor: colors.border.subtle,
          },
        ]}
      >
        <Text
          style={[
            styles.title,
            { color: colors.text.primary },
          ]}
        >
          🧪 Live Props (DEV)
        </Text>

        <Text
          style={[
            styles.subtitle,
            { color: colors.text.muted },
          ]}
        >
          Simulated live prop feed for testing
        </Text>
      </Pressable>

      {/* Opponent Position Defense */}
      <Pressable
        onPress={() =>
          router.push("/(tabs)/more/opponent-position-defense")
        }
        style={[
          styles.card,
          {
            backgroundColor: colors.surface.card,
            borderColor: colors.border.subtle,
          },
        ]}
      >
        <Text
          style={[
            styles.title,
            { color: colors.text.primary },
          ]}
        >
          🛡️ Opponent Position Defense
        </Text>

        <Text
          style={[
            styles.subtitle,
            { color: colors.text.muted },
          ]}
        >
          See opponent averages allowed by position
        </Text>
      </Pressable>
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    padding: 12,
    gap: 12,
  },

  card: {
    padding: 16,
    borderRadius: 16,
    borderWidth: 1,
  },

  title: {
    fontSize: 16,
    fontWeight: "900",
  },

  subtitle: {
    fontSize: 12,
    marginTop: 4,
    fontWeight: "600",
  },
});

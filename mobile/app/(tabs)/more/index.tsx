// app/(tabs)/more/index.tsx
import { View, Text, Pressable, StyleSheet, ScrollView } from "react-native";
import { useRouter } from "expo-router";

import { useTheme } from "@/store/useTheme";
import { useParlayTracker } from "@/store/useParlayTracker";

export default function MoreIndexScreen() {
  const { colors } = useTheme();
  const router = useRouter();

  const { tracked } = useParlayTracker();
  const trackedCount = Object.keys(tracked).length;

  return (
    <ScrollView
      style={{ flex: 1, backgroundColor: colors.surface.base }}
      contentContainerStyle={styles.container}
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

      {/* Prop Correlations */}
      <Pressable
        onPress={() =>
          router.push("/(tabs)/more/correlations")
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
          🔗 Prop Correlations
        </Text>

        <Text
          style={[
            styles.subtitle,
            { color: colors.text.muted },
          ]}
        >
          Teammate prop pairs that tend to hit together
        </Text>
      </Pressable>

      {/* Game Environment */}
      <Pressable
        onPress={() =>
          router.push("/(tabs)/more/game-environment")
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
          🌡️ Game Environment
        </Text>

        <Text
          style={[
            styles.subtitle,
            { color: colors.text.muted },
          ]}
        >
          Pace, totals, rest & scoring context for tonight
        </Text>
      </Pressable>

      {/* Player Season Averages */}
      <Pressable
        onPress={() =>
          router.push("/(tabs)/more/player-season-averages")
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
          🏀 Player Season Averages
        </Text>

        <Text
          style={[
            styles.subtitle,
            { color: colors.text.muted },
          ]}
        >
          Search and view player stats for 2024-25
        </Text>
      </Pressable>

      {/* Team Season Averages */}
      <Pressable
        onPress={() =>
          router.push("/(tabs)/more/team-season-averages")
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
          🏟️ Team Season Averages
        </Text>

        <Text
          style={[
            styles.subtitle,
            { color: colors.text.muted },
          ]}
        >
          View team standings and stats for 2024-25
        </Text>
      </Pressable>

      {/* 100 in 3Q */}
      <Pressable
        onPress={() =>
          router.push("/(tabs)/more/three-quarter-100")
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
          🔥 100 in 3Q
        </Text>

        <Text
          style={[
            styles.subtitle,
            { color: colors.text.muted },
          ]}
        >
          Teams most likely to hit 100 by the 3rd
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
    </ScrollView>
  );
}

const styles = StyleSheet.create({
  container: {
    padding: 12,
    gap: 12,
    paddingBottom: 24,
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

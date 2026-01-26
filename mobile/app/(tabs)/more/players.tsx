import { View, ActivityIndicator, Text, StyleSheet } from "react-native";
import { useTheme } from "@/store/useTheme";
import { PlayerSeasonMegaTable } from "@/components/table/PlayerSeasonMegaTable";
import { usePlayerSeasonMega } from "@/hooks/usePlayerSeasonMega";

export default function PlayersMegaScreen() {
  const { colors } = useTheme();
  const { rows, loading, error } = usePlayerSeasonMega({
    limit: 500,
  });

  if (loading) {
    return (
      <View
        style={[
          styles.center,
          { backgroundColor: colors.surface.screen },
        ]}
      >
        <ActivityIndicator color={colors.accent.primary} />
      </View>
    );
  }

  if (error) {
    return (
      <View
        style={[
          styles.center,
          { backgroundColor: colors.surface.screen },
        ]}
      >
        <Text style={{ color: colors.text.muted }}>
          Failed to load players: {error}
        </Text>
      </View>
    );
  }

  return (
    <View
      style={[
        styles.container,
        { backgroundColor: colors.surface.screen },
      ]}
    >
      <PlayerSeasonMegaTable rows={rows} />
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
  },
  center: {
    flex: 1,
    alignItems: "center",
    justifyContent: "center",
  },
});
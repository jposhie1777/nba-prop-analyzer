import { View, Text, FlatList, StyleSheet, ActivityIndicator, RefreshControl } from "react-native";
import { useTheme } from "@/store/useTheme";
import { useLadders } from "@/hooks/useLadders";
import { LadderCard } from "@/components/ladders/LadderCard";

export function LaddersScreen() {
  const { colors } = useTheme();
  const { data, loading, error, refetch } = useLadders();

  if (loading && data.length === 0) {
    return (
      <View style={[styles.center, { backgroundColor: colors.surface.background }]}>
        <ActivityIndicator size="large" color={colors.text.secondary} />
        <Text style={[styles.loadingText, { color: colors.text.muted }]}>
          Loading ladders…
        </Text>
      </View>
    );
  }

  if (error) {
    return (
      <View style={[styles.center, { backgroundColor: colors.surface.background }]}>
        <Text style={[styles.errorText, { color: colors.text.primary }]}>
          Failed to load ladders
        </Text>
        <Text style={[styles.errorDetail, { color: colors.text.muted }]}>
          {error.message}
        </Text>
      </View>
    );
  }

  if (data.length === 0) {
    return (
      <View style={[styles.center, { backgroundColor: colors.surface.background }]}>
        <Text style={[styles.emptyText, { color: colors.text.primary }]}>
          No ladders available
        </Text>
        <Text style={[styles.emptyDetail, { color: colors.text.muted }]}>
          Check back when games are live
        </Text>
      </View>
    );
  }

  return (
    <FlatList
      data={data}
      keyExtractor={(item) =>
        `${item.game_id}-${item.player_id}-${item.market}`
      }
      renderItem={({ item }) => <LadderCard ladder={item} />}
      contentContainerStyle={styles.list}
      style={{ backgroundColor: colors.surface.background }}
      refreshControl={
        <RefreshControl
          refreshing={loading}
          onRefresh={refetch}
          tintColor={colors.text.secondary}
        />
      }
    />
  );
}

const styles = StyleSheet.create({
  list: {
    padding: 12,
  },
  center: {
    flex: 1,
    alignItems: "center",
    justifyContent: "center",
    padding: 20,
  },
  loadingText: {
    marginTop: 12,
    fontSize: 14,
  },
  errorText: {
    fontSize: 18,
    fontWeight: "600",
    marginBottom: 8,
  },
  errorDetail: {
    fontSize: 14,
    textAlign: "center",
  },
  emptyText: {
    fontSize: 18,
    fontWeight: "600",
    marginBottom: 8,
  },
  emptyDetail: {
    fontSize: 14,
    textAlign: "center",
  },
});
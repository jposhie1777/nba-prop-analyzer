import { View, Text, FlatList, StyleSheet } from "react-native";
import { useLivePropsDev } from "@/hooks/useLivePropsDev";
import { useTheme } from "@/store/useTheme";
import LivePropCard from "@/components/live/LivePropCard";

export default function LivePropsDevScreen() {
  const { colors } = useTheme();
  const { data, isLoading, error } = useLivePropsDev(50);

  if (isLoading) {
    return (
      <View
        style={[
          styles.center,
          { backgroundColor: colors.surface.screen },
        ]}
      >
        <Text style={{ color: colors.text.muted }}>
          Loading live props…
        </Text>
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
        <Text style={{ color: colors.accent.danger }}>
          Error loading live props
        </Text>
      </View>
    );
  }

  if (!data || data.length === 0) {
    return (
      <View
        style={[
          styles.center,
          { backgroundColor: colors.surface.screen },
        ]}
      >
        <Text style={{ color: colors.text.muted }}>
          No live props available
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
      <FlatList
        data={data}
        keyExtractor={(item) =>
          `${item.game_id}-${item.player_id}-${item.market}-${item.line}-${item.book}`
        }
        contentContainerStyle={{ padding: 12 }}
        renderItem={({ item }) => (
          <LivePropCard item={item} />
        )}
      />
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
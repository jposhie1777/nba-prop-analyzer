// mobile/app/(tabs)/live-props-dev.tsx
import { View, Text, FlatList, StyleSheet } from "react-native";
import { useLivePropsDev } from "@/hooks/useLivePropsDev";
import { useTheme } from "@/store/useTheme";

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
        <Text style={{ color: colors.text.danger }}>
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
          <View
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
              {item.market.toUpperCase()} · {item.line}
            </Text>

            <Text
              style={[
                styles.body,
                { color: colors.text.secondary },
              ]}
            >
              Current: {item.current_stat} → Need{" "}
              {item.remaining_needed}
            </Text>

            <Text
              style={[
                styles.meta,
                { color: colors.text.muted },
              ]}
            >
              Book: {item.book}
            </Text>
          </View>
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

  card: {
    padding: 14,
    borderRadius: 14,
    borderWidth: 1,
    marginBottom: 12,
  },

  title: {
    fontSize: 14,
    fontWeight: "900",
  },

  body: {
    marginTop: 6,
    fontSize: 13,
    fontWeight: "600",
  },

  meta: {
    marginTop: 4,
    fontSize: 11,
    fontWeight: "600",
  },
});
import { View, Text, FlatList, StyleSheet } from "react-native";
import { useTheme } from "@/store/useTheme";
import { useLadders } from "@/hooks/useLadders";
import { LadderCard } from "@/components/ladders/LadderCard";

export function LaddersScreen() {
  const { colors } = useTheme();
  const { data, loading } = useLadders();

  if (loading) {
    return (
      <View style={styles.center}>
        <Text style={{ color: colors.text }}>Loading ladders…</Text>
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
    />
  );
}

const styles = StyleSheet.create({
  list: {
    padding: 12,
    gap: 12,
  },
  center: {
    flex: 1,
    alignItems: "center",
    justifyContent: "center",
  },
});
// mobile/app/(tabs)/live-props-dev.tsx

import { View, Text, FlatList } from "react-native";
import { useLivePropsDev } from "@/hooks/useLivePropsDev";

export default function LivePropsDevScreen() {
  const { data, isLoading, error } = useLivePropsDev(50);

  if (isLoading) return <Text>Loading…</Text>;
  if (error) return <Text>Error loading</Text>;

  return (
    <FlatList
      data={data}
      keyExtractor={(item) =>
        `${item.game_id}-${item.player_id}-${item.market}-${item.line}-${item.book}`
      }
      renderItem={({ item }) => (
        <View style={{ padding: 12 }}>
          <Text>
            {item.market.toUpperCase()} {item.line}
          </Text>
          <Text>
            Current: {item.current_stat} → Need {item.remaining_needed}
          </Text>
          <Text>{item.book}</Text>
        </View>
      )}
    />
  );
}

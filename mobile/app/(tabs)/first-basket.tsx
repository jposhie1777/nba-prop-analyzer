// /app/(tabs)first-basket.tsx
import React from "react";
import { View, Text, FlatList, RefreshControl } from "react-native";
import { useTheme } from "@/store/useTheme";
import { useFirstBasketMatchups } from "@/hooks/useFirstBasketMatchups";
import { FirstBasketMatchupCard } from "@/components/first-basket/FirstBasketMatchupCard";

export default function FirstBasketScreen() {
  const { colors } = useTheme();
  const { data, loading, error, refresh } = useFirstBasketMatchups();

  return (
    <View style={{ flex: 1, backgroundColor: colors.bg }}>
      {error ? (
        <Text style={{ color: colors.danger, padding: 12 }}>Error: {error}</Text>
      ) : null}

      <FlatList
        contentContainerStyle={{ padding: 12, paddingBottom: 40 }}
        data={data}
        keyExtractor={(m) => String(m.gameId)}
        renderItem={({ item }) => <FirstBasketMatchupCard matchup={item} />}
        refreshControl={<RefreshControl refreshing={loading} onRefresh={refresh} />}
      />
    </View>
  );
}

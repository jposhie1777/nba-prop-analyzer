import { ScrollView, View, Text, ActivityIndicator } from "react-native";
import { Stack } from "expo-router";
import { useEffect, useMemo, useState } from "react";

import { useTheme } from "@/store/useTheme";
import { usePgaPlayers } from "@/hooks/pga/usePgaPlayers";
import { usePgaQuery } from "@/hooks/pga/usePgaQuery";
import { MetricCard } from "@/components/pga/MetricCard";
import { SearchPicker, SearchItem } from "@/components/pga/SearchPicker";
import { PgaSimulatedFinishes } from "@/types/pga";

export default function SimulatedFinishesScreen() {
  const { colors } = useTheme();
  const [search, setSearch] = useState("");
  const [selected, setSelected] = useState<SearchItem | null>(null);

  const { data: playersData } = usePgaPlayers({ search });
  const playerItems = useMemo(
    () =>
      (playersData?.data || []).map((player) => ({
        id: player.id,
        label: player.display_name,
        subLabel: player.country ?? "",
      })),
    [playersData]
  );

  useEffect(() => {
    if (!selected && playerItems.length > 0) {
      setSelected(playerItems[0]);
    }
  }, [playerItems, selected]);

  const { data, loading, error } = usePgaQuery<PgaSimulatedFinishes>(
    "/pga/analytics/simulated-finishes",
    { player_id: selected?.id, last_n: 20, simulations: 2000 },
    !!selected
  );

  const distributionEntries = useMemo(
    () =>
      data?.distribution
        ? Object.entries(data.distribution).map(([key, value]) => ({
            label: key,
            value,
          }))
        : [],
    [data]
  );

  return (
    <>
      <Stack.Screen
        options={{
          title: "Simulated Finishes",
          headerStyle: { backgroundColor: colors.surface.screen },
          headerTintColor: colors.text.primary,
        }}
      />
      <ScrollView
        style={{ flex: 1, backgroundColor: colors.surface.screen }}
        contentContainerStyle={{ padding: 16, paddingBottom: 40 }}
      >
        <SearchPicker
          title="Player"
          placeholder="Search players..."
          query={search}
          onQueryChange={setSearch}
          items={playerItems}
          selectedId={selected?.id}
          onSelect={setSelected}
        />

        {loading ? (
          <View style={{ padding: 20, alignItems: "center" }}>
            <ActivityIndicator color={colors.accent.primary} />
          </View>
        ) : null}

        {error ? (
          <Text style={{ color: colors.text.danger, marginTop: 12 }}>
            {error}
          </Text>
        ) : null}

        {data ? (
          <MetricCard
            title="Simulation Summary"
            subtitle={`Simulations: ${data.simulations}`}
            metrics={[
              { label: "Top 5", value: data.top5_prob },
              { label: "Top 10", value: data.top10_prob },
              { label: "Top 20", value: data.top20_prob },
            ]}
          />
        ) : null}

        {distributionEntries.map((entry) => (
          <MetricCard
            key={entry.label}
            title={entry.label}
            metrics={[{ label: "Probability", value: entry.value }]}
          />
        ))}
      </ScrollView>
    </>
  );
}

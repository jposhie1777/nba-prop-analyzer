import { ScrollView, View, Text, ActivityIndicator } from "react-native";
import { Stack } from "expo-router";
import { useEffect, useMemo, useState } from "react";

import { useTheme } from "@/store/useTheme";
import { usePgaPlayers } from "@/hooks/pga/usePgaPlayers";
import { usePgaQuery } from "@/hooks/pga/usePgaQuery";
import { MetricCard } from "@/components/pga/MetricCard";
import { SearchPicker, SearchItem } from "@/components/pga/SearchPicker";
import { PgaRegionSplitsResponse } from "@/types/pga";

export default function RegionSplitsScreen() {
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

  const { data, loading, error } = usePgaQuery<PgaRegionSplitsResponse>(
    "/pga/analytics/region-splits",
    { player_id: selected?.id },
    !!selected
  );

  return (
    <>
      <Stack.Screen
        options={{
          title: "Region / Time Splits",
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

        <Text style={{ color: colors.text.primary, fontWeight: "700", marginTop: 8 }}>
          By Month
        </Text>
        {(data?.by_month || []).map((row) => (
          <MetricCard
            key={`month-${row.key}`}
            title={`Month ${row.key}`}
            metrics={[
              { label: "Starts", value: row.starts },
              { label: "Avg Finish", value: row.avg_finish },
              { label: "Top 10 Rate", value: row.top10_rate },
            ]}
          />
        ))}

        <Text style={{ color: colors.text.primary, fontWeight: "700", marginTop: 8 }}>
          By Country
        </Text>
        {(data?.by_country || []).slice(0, 10).map((row) => (
          <MetricCard
            key={`country-${row.key}`}
            title={String(row.key)}
            metrics={[
              { label: "Starts", value: row.starts },
              { label: "Avg Finish", value: row.avg_finish },
              { label: "Top 10 Rate", value: row.top10_rate },
            ]}
          />
        ))}
      </ScrollView>
    </>
  );
}

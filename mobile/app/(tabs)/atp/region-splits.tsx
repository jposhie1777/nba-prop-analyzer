import { ScrollView, View, Text, ActivityIndicator } from "react-native";
import { Stack } from "expo-router";
import { useEffect, useMemo, useState } from "react";

import { useTheme } from "@/store/useTheme";
import { useAtpPlayers } from "@/hooks/atp/useAtpPlayers";
import { useAtpQuery } from "@/hooks/atp/useAtpQuery";
import { MetricCard } from "@/components/pga/MetricCard";
import { SearchPicker, SearchItem } from "@/components/pga/SearchPicker";
import { AtpRegionSplitsResponse } from "@/types/atp";

export default function AtpRegionSplitsScreen() {
  const { colors } = useTheme();
  const [search, setSearch] = useState("");
  const [player, setPlayer] = useState<SearchItem | null>(null);

  const { data: players } = useAtpPlayers({ search });
  const items = useMemo(
    () =>
      (players?.data || []).map((entry) => ({
        id: entry.id,
        label: entry.full_name || `${entry.first_name ?? ""} ${entry.last_name ?? ""}`,
        subLabel: entry.country ?? "",
      })),
    [players]
  );

  useEffect(() => {
    if (!player && items.length > 0) {
      setPlayer(items[0]);
    }
  }, [items, player]);

  const { data, loading, error } = useAtpQuery<AtpRegionSplitsResponse>(
    "/atp/analytics/region-splits",
    { player_id: player?.id, seasons_back: 3 },
    !!player
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
          items={items}
          selectedId={player?.id}
          onSelect={setPlayer}
          helperText="Win rates by month and tournament location."
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

        <Text style={{ color: colors.text.primary, fontWeight: "700", marginTop: 6 }}>
          By Month
        </Text>
        {(data?.by_month || []).map((row) => (
          <MetricCard
            key={`month-${row.key}`}
            title={`Month ${row.key}`}
            subtitle={`Matches: ${row.matches}`}
            metrics={[
              { label: "Wins", value: row.wins },
              { label: "Win Rate", value: row.win_rate },
            ]}
          />
        ))}

        <Text style={{ color: colors.text.primary, fontWeight: "700", marginTop: 12 }}>
          By Location
        </Text>
        {(data?.by_location || []).slice(0, 10).map((row) => (
          <MetricCard
            key={`loc-${row.key}`}
            title={String(row.key)}
            subtitle={`Matches: ${row.matches}`}
            metrics={[
              { label: "Wins", value: row.wins },
              { label: "Win Rate", value: row.win_rate },
            ]}
          />
        ))}
      </ScrollView>
    </>
  );
}

import { ScrollView, View, Text, TextInput, ActivityIndicator } from "react-native";
import { Stack } from "expo-router";
import { useEffect, useMemo, useState } from "react";

import { useTheme } from "@/store/useTheme";
import { useAtpPlayers } from "@/hooks/atp/useAtpPlayers";
import { useAtpQuery } from "@/hooks/atp/useAtpQuery";
import { MetricCard } from "@/components/pga/MetricCard";
import { SearchPicker, SearchItem } from "@/components/pga/SearchPicker";
import { AtpSurfaceSplitRow } from "@/types/atp";

type Response = {
  player_id: number;
  rows: AtpSurfaceSplitRow[];
};

export default function AtpSurfaceSplitsScreen() {
  const { colors } = useTheme();
  const [search, setSearch] = useState("");
  const [player, setPlayer] = useState<SearchItem | null>(null);
  const [seasonsBack, setSeasonsBack] = useState("2");

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

  const seasonsBackNum = useMemo(
    () => Number(seasonsBack) || 0,
    [seasonsBack]
  );

  const { data, loading, error } = useAtpQuery<Response>(
    "/atp/analytics/surface-splits",
    {
      player_id: player?.id,
      seasons_back: seasonsBackNum,
      min_matches: 5,
    },
    !!player
  );

  return (
    <>
      <Stack.Screen
        options={{
          title: "Surface Splits",
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
          helperText="Surface performance across recent seasons."
        />

        <Text style={{ color: colors.text.primary, fontWeight: "700" }}>
          Seasons Back
        </Text>
        <TextInput
          value={seasonsBack}
          onChangeText={setSeasonsBack}
          keyboardType="number-pad"
          style={{
            marginTop: 6,
            borderWidth: 1,
            borderColor: colors.border.subtle,
            borderRadius: 10,
            paddingHorizontal: 12,
            paddingVertical: 10,
            backgroundColor: colors.surface.card,
            color: colors.text.primary,
          }}
          placeholderTextColor={colors.text.muted}
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

        {(data?.rows || []).map((row) => (
          <MetricCard
            key={row.surface}
            title={row.surface}
            subtitle={`Matches: ${row.matches} | Wins: ${row.wins}`}
            metrics={[
              { label: "Win Rate", value: row.win_rate },
              { label: "Straight Sets", value: row.straight_sets_rate },
              { label: "Avg Sets", value: row.avg_sets },
              { label: "Tiebreak Rate", value: row.tiebreak_rate },
            ]}
          />
        ))}

        {!loading && !error && data?.rows?.length === 0 ? (
          <Text style={{ color: colors.text.muted, marginTop: 20 }}>
            No surface data for this player.
          </Text>
        ) : null}
      </ScrollView>
    </>
  );
}

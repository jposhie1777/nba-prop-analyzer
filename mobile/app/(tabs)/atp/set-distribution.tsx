import { ScrollView, View, Text, TextInput, ActivityIndicator } from "react-native";
import { Stack } from "expo-router";
import { useEffect, useMemo, useState } from "react";

import { useTheme } from "@/store/useTheme";
import { useAtpPlayers } from "@/hooks/atp/useAtpPlayers";
import { useAtpQuery } from "@/hooks/atp/useAtpQuery";
import { MetricCard } from "@/components/pga/MetricCard";
import { SearchPicker, SearchItem } from "@/components/pga/SearchPicker";
import { AtpSetDistribution } from "@/types/atp";

export default function AtpSetDistributionScreen() {
  const { colors } = useTheme();
  const [search, setSearch] = useState("");
  const [player, setPlayer] = useState<SearchItem | null>(null);
  const [surface, setSurface] = useState("");

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

  const { data, loading, error } = useAtpQuery<AtpSetDistribution>(
    "/atp/analytics/set-distribution",
    {
      player_id: player?.id,
      seasons_back: 3,
      surface: surface.trim() || undefined,
    },
    !!player
  );

  return (
    <>
      <Stack.Screen
        options={{
          title: "Set Distribution",
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
          helperText="Set-score outcomes for wins and losses."
        />

        <Text style={{ color: colors.text.primary, fontWeight: "700" }}>
          Surface (optional)
        </Text>
        <TextInput
          value={surface}
          onChangeText={setSurface}
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
          placeholder="Hard, Clay, Grass"
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

        <Text style={{ color: colors.text.primary, fontWeight: "700", marginTop: 12 }}>
          Wins
        </Text>
        {Object.entries(data?.wins || {}).map(([label, count]) => (
          <MetricCard
            key={`win-${label}`}
            title={`Set Score ${label}`}
            subtitle={`Count: ${count}`}
            metrics={[
              { label: "Win Rate", value: data?.win_rates?.[label] ?? 0 },
            ]}
          />
        ))}

        <Text style={{ color: colors.text.primary, fontWeight: "700", marginTop: 12 }}>
          Losses
        </Text>
        {Object.entries(data?.losses || {}).map(([label, count]) => (
          <MetricCard
            key={`loss-${label}`}
            title={`Set Score ${label}`}
            subtitle={`Count: ${count}`}
            metrics={[
              { label: "Loss Rate", value: data?.loss_rates?.[label] ?? 0 },
            ]}
          />
        ))}
      </ScrollView>
    </>
  );
}

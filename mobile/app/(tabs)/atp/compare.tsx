import { ScrollView, View, Text, TextInput, ActivityIndicator } from "react-native";
import { Stack } from "expo-router";
import { useEffect, useMemo, useState } from "react";

import { useTheme } from "@/store/useTheme";
import { useAtpPlayers } from "@/hooks/atp/useAtpPlayers";
import { useAtpQuery } from "@/hooks/atp/useAtpQuery";
import { SearchPicker, SearchItem } from "@/components/pga/SearchPicker";
import { MetricCard } from "@/components/pga/MetricCard";
import { AtpCompareResponse } from "@/types/atp";

const formatPct = (value?: number | null) =>
  value == null ? "—" : `${(value * 100).toFixed(1)}%`;

const formatNum = (value?: number | null) =>
  value == null ? "—" : value.toFixed(2);

export default function AtpCompareScreen() {
  const { colors } = useTheme();
  const [searchA, setSearchA] = useState("");
  const [searchB, setSearchB] = useState("");
  const [playerA, setPlayerA] = useState<SearchItem | null>(null);
  const [playerB, setPlayerB] = useState<SearchItem | null>(null);
  const [surface, setSurface] = useState("");

  const { data: playersA } = useAtpPlayers({ search: searchA });
  const { data: playersB } = useAtpPlayers({ search: searchB });

  const itemsA = useMemo(
    () =>
      (playersA?.data || []).map((player) => ({
        id: player.id,
        label: player.full_name || `${player.first_name ?? ""} ${player.last_name ?? ""}`,
        subLabel: player.country ?? "",
      })),
    [playersA]
  );
  const itemsB = useMemo(
    () =>
      (playersB?.data || []).map((player) => ({
        id: player.id,
        label: player.full_name || `${player.first_name ?? ""} ${player.last_name ?? ""}`,
        subLabel: player.country ?? "",
      })),
    [playersB]
  );

  useEffect(() => {
    if (!playerA && itemsA.length > 0) {
      setPlayerA(itemsA[0]);
    }
  }, [itemsA, playerA]);

  useEffect(() => {
    if (!playerB && itemsB.length > 0) {
      // Avoid auto-selecting the same player as Player A
      const candidate = playerA
        ? itemsB.find((item) => item.id !== playerA.id) ?? null
        : itemsB[0];
      if (candidate) setPlayerB(candidate);
    }
  }, [itemsB, playerB, playerA]);

  const playerIds = useMemo(
    () => [playerA?.id, playerB?.id].filter((id): id is number => !!id),
    [playerA, playerB]
  );

  const hasTwoDistinctPlayers =
    playerIds.length === 2 && playerIds[0] !== playerIds[1];

  const { data, loading, error } = useAtpQuery<AtpCompareResponse>(
    "/atp/analytics/compare",
    {
      player_ids: playerIds,
      surface: surface.trim() || undefined,
    },
    hasTwoDistinctPlayers
  );

  const playerNameMap = useMemo(() => {
    const map = new Map<number, string>();
    if (playerA) map.set(playerA.id, playerA.label);
    if (playerB) map.set(playerB.id, playerB.label);
    return map;
  }, [playerA, playerB]);

  return (
    <>
      <Stack.Screen
        options={{
          title: "Matchup Compare",
          headerStyle: { backgroundColor: colors.surface.screen },
          headerTintColor: colors.text.primary,
        }}
      />
      <ScrollView
        style={{ flex: 1, backgroundColor: colors.surface.screen }}
        contentContainerStyle={{ padding: 16, paddingBottom: 40 }}
      >
        <SearchPicker
          title="Player A"
          placeholder="Search player..."
          query={searchA}
          onQueryChange={setSearchA}
          items={itemsA}
          selectedId={playerA?.id}
          onSelect={setPlayerA}
        />
        <SearchPicker
          title="Player B"
          placeholder="Search player..."
          query={searchB}
          onQueryChange={setSearchB}
          items={itemsB}
          selectedId={playerB?.id}
          onSelect={setPlayerB}
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

        {data?.recommendation ? (
          <MetricCard
            title="Recommendation"
            subtitle={data.recommendation.label}
            metrics={[
              {
                label: "Best Play",
                value: playerNameMap.get(data.recommendation.player_id) ?? data.recommendation.player_id,
              },
              { label: "Edge", value: formatNum(data.recommendation.edge) },
              { label: "Reason", value: data.recommendation.reasons.join(" • ") },
            ]}
          />
        ) : null}

        {(data?.players || []).map((row) => {
          const hasRich = row.metrics.l15_win_rate != null;
          const primaryMetrics = [
            { label: "Form Score", value: formatNum(row.metrics.form_score) },
            { label: "L15 Win %", value: formatPct(row.metrics.l15_win_rate ?? row.metrics.recent_win_rate) },
            { label: "Surface Win %", value: formatPct(row.metrics.surface_win_rate) },
            { label: "Ranking", value: row.metrics.ranking ?? "—" },
          ];
          const expandedMetrics = hasRich ? [
            { label: "L40 Win %", value: formatPct(row.metrics.l40_win_rate) },
            { label: "vs Top 50", value: formatPct(row.metrics.win_rate_vs_top50) },
            { label: "Str. Sets %", value: formatPct(row.metrics.straight_sets_win_rate) },
            { label: "Tiebreak %", value: formatPct(row.metrics.tiebreak_rate) },
            { label: "Streak", value: (row.metrics.current_win_streak ?? 0) > 0 ? `W${row.metrics.current_win_streak}` : (row.metrics.current_loss_streak ?? 0) > 0 ? `L${row.metrics.current_loss_streak}` : "—" },
            { label: "Confidence", value: (row.metrics.sample_confidence ?? 0) >= 0.85 ? "High" : (row.metrics.sample_confidence ?? 0) >= 0.65 ? "Med" : "Low" },
          ] : [];
          return (
            <MetricCard
              key={row.player_id}
              title={`${row.rank}. ${playerNameMap.get(row.player_id) ?? row.player_id}`}
              subtitle={`Score: ${formatNum(row.score)}`}
              metrics={primaryMetrics}
              expandedMetrics={expandedMetrics}
            />
          );
        })}
      </ScrollView>
    </>
  );
}

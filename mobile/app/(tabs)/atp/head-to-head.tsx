import { ScrollView, View, Text, ActivityIndicator } from "react-native";
import { Stack } from "expo-router";
import { useEffect, useMemo, useState } from "react";

import { useTheme } from "@/store/useTheme";
import { useAtpPlayers } from "@/hooks/atp/useAtpPlayers";
import { useAtpQuery } from "@/hooks/atp/useAtpQuery";
import { MetricCard } from "@/components/pga/MetricCard";
import { SearchPicker, SearchItem } from "@/components/pga/SearchPicker";
import { AtpHeadToHeadResponse } from "@/types/atp";

export default function AtpHeadToHeadScreen() {
  const { colors } = useTheme();
  const [searchA, setSearchA] = useState("");
  const [searchB, setSearchB] = useState("");
  const [playerA, setPlayerA] = useState<SearchItem | null>(null);
  const [playerB, setPlayerB] = useState<SearchItem | null>(null);

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
      setPlayerB(itemsB[0]);
    }
  }, [itemsB, playerB]);

  const { data, loading, error } = useAtpQuery<AtpHeadToHeadResponse>(
    "/atp/analytics/head-to-head",
    {
      player_id: playerA?.id,
      opponent_id: playerB?.id,
      seasons_back: 4,
    },
    !!playerA && !!playerB
  );

  return (
    <>
      <Stack.Screen
        options={{
          title: "Head-to-Head",
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
          placeholder="Search players..."
          query={searchA}
          onQueryChange={setSearchA}
          items={itemsA}
          selectedId={playerA?.id}
          onSelect={setPlayerA}
        />
        <SearchPicker
          title="Player B"
          placeholder="Search players..."
          query={searchB}
          onQueryChange={setSearchB}
          items={itemsB}
          selectedId={playerB?.id}
          onSelect={setPlayerB}
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
            title="Series Summary"
            subtitle="Across selected seasons"
            metrics={[
              { label: "Starts", value: data.starts },
              { label: "Wins", value: data.wins },
              { label: "Losses", value: data.losses },
              { label: "Win Rate", value: data.win_rate },
            ]}
          />
        ) : null}

        {(data?.by_surface || []).map((row) => (
          <MetricCard
            key={row.surface}
            title={`${row.surface} Courts`}
            subtitle={`Matches: ${row.matches}`}
            metrics={[
              { label: "Wins", value: row.wins },
              { label: "Losses", value: row.losses },
              { label: "Win Rate", value: row.win_rate },
            ]}
          />
        ))}

        {(data?.matches || []).slice(0, 10).map((match, index) => (
          <MetricCard
            key={`${match.tournament?.id ?? index}`}
            title={match.tournament?.name ?? "Tournament"}
            subtitle={`${match.round ?? ""} ${match.start_date ?? ""}`.trim()}
            metrics={[
              { label: "Surface", value: match.surface },
              { label: "Result", value: match.result },
              { label: "Score", value: match.score },
            ]}
          />
        ))}
      </ScrollView>
    </>
  );
}

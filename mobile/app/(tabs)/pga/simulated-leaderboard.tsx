import { ScrollView, Text, TextInput, View, ActivityIndicator } from "react-native";
import { Stack } from "expo-router";
import { useMemo, useState } from "react";

import { useTheme } from "@/store/useTheme";
import { usePgaQuery } from "@/hooks/pga/usePgaQuery";
import { MetricCard } from "@/components/pga/MetricCard";
import { PgaSimulatedLeaderboard } from "@/types/pga";

export default function SimulatedLeaderboardScreen() {
  const { colors } = useTheme();
  const [season, setSeason] = useState(String(new Date().getFullYear() - 1));
  const seasonNum = useMemo(() => Number(season) || undefined, [season]);

  const { data, loading, error } = usePgaQuery<PgaSimulatedLeaderboard>(
    "/pga/analytics/simulated-leaderboard",
    { season: seasonNum, last_n: 20, min_events: 5, simulations: 2000 }
  );

  return (
    <>
      <Stack.Screen
        options={{
          title: "Simulated Leaderboard",
          headerStyle: { backgroundColor: colors.surface.screen },
          headerTintColor: colors.text.primary,
        }}
      />
      <ScrollView
        style={{ flex: 1, backgroundColor: colors.surface.screen }}
        contentContainerStyle={{ padding: 16, paddingBottom: 40 }}
      >
        <Text style={{ color: colors.text.primary, fontWeight: "700" }}>Season</Text>
        <TextInput
          value={season}
          onChangeText={setSeason}
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

        <Text style={{ color: colors.text.muted, marginTop: 8 }}>
          Tournament-wide simulation to quickly scan projected finishes and top-x hit rates.
        </Text>

        {loading ? (
          <View style={{ padding: 20, alignItems: "center" }}>
            <ActivityIndicator color={colors.accent.primary} />
          </View>
        ) : null}

        {error ? <Text style={{ color: colors.text.danger, marginTop: 12 }}>{error}</Text> : null}

        {data ? (
          <MetricCard
            title="Field Summary"
            subtitle={`Players: ${data.field_size}`}
            metrics={[{ label: "Simulations", value: data.simulations }]}
          />
        ) : null}

        {(data?.leaderboard || []).slice(0, 40).map((row, idx) => (
          <MetricCard
            key={row.player_id}
            title={`${idx + 1}. ${row.player.display_name}`}
            subtitle={`Starts used: ${row.starts}`}
            metrics={[
              { label: "Projected Finish", value: row.projected_finish },
              { label: "Win", value: row.win_prob },
              { label: "Top 5", value: row.top5_prob },
              { label: "Top 10", value: row.top10_prob },
              { label: "Top 20", value: row.top20_prob },
            ]}
          />
        ))}
      </ScrollView>
    </>
  );
}

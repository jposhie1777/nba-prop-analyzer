import { ScrollView, View, Text, TextInput, ActivityIndicator } from "react-native";
import { Stack } from "expo-router";
import { useMemo, useState } from "react";

import { useTheme } from "@/store/useTheme";
import { usePgaQuery } from "@/hooks/pga/usePgaQuery";
import { MetricCard } from "@/components/pga/MetricCard";
import { PgaPlayerFormRow } from "@/types/pga";

type Response = {
  season: number;
  rows: PgaPlayerFormRow[];
};

export default function PlayerFormScreen() {
  const { colors } = useTheme();
  const [season, setSeason] = useState(String(new Date().getFullYear()));
  const seasonNum = useMemo(() => Number(season) || undefined, [season]);

  const { data, loading, error } = usePgaQuery<Response>(
    "/pga/analytics/player-form",
    { season: seasonNum, last_n: 10, min_events: 3 }
  );

  return (
    <>
      <Stack.Screen
        options={{
          title: "Player Form + Consistency",
          headerStyle: { backgroundColor: colors.surface.screen },
          headerTintColor: colors.text.primary,
        }}
      />
      <ScrollView
        style={{ flex: 1, backgroundColor: colors.surface.screen }}
        contentContainerStyle={{ padding: 16, paddingBottom: 40 }}
      >
        <Text style={{ color: colors.text.primary, fontWeight: "700" }}>
          Season
        </Text>
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
        <Text style={{ color: colors.text.muted, marginTop: 6, fontSize: 12 }}>
          Based on last 10 starts in the selected season.
        </Text>

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

        {(data?.rows || []).slice(0, 25).map((row) => (
          <MetricCard
            key={row.player_id}
            title={row.player.display_name}
            subtitle={`Starts: ${row.starts} | Recent: ${row.recent_finishes.join(", ")}`}
            metrics={[
              { label: "Avg Finish", value: row.avg_finish },
              { label: "Top 10", value: row.top10_rate },
              { label: "Top 20", value: row.top20_rate },
              { label: "Cut Rate", value: row.cut_rate },
              { label: "Consistency", value: row.consistency_index },
              { label: "Form Score", value: row.form_score },
            ]}
          />
        ))}

        {!loading && !error && data?.rows?.length === 0 ? (
          <Text style={{ color: colors.text.muted, marginTop: 20 }}>
            No results for this season.
          </Text>
        ) : null}
      </ScrollView>
    </>
  );
}

import { ScrollView, View, Text, TextInput, ActivityIndicator } from "react-native";
import { Stack } from "expo-router";
import { useMemo, useState } from "react";

import { useTheme } from "@/store/useTheme";
import { usePgaQuery } from "@/hooks/pga/usePgaQuery";
import { MetricCard } from "@/components/pga/MetricCard";
import { PgaPlacementRow } from "@/types/pga";

type Response = {
  season: number;
  rows: PgaPlacementRow[];
};

export default function PlacementProbabilitiesScreen() {
  const { colors } = useTheme();
  const [season, setSeason] = useState(String(new Date().getFullYear()));
  const seasonNum = useMemo(() => Number(season) || undefined, [season]);

  const { data, loading, error } = usePgaQuery<Response>(
    "/pga/analytics/placement-probabilities",
    { season: seasonNum, last_n: 20, min_events: 5 }
  );

  return (
    <>
      <Stack.Screen
        options={{
          title: "Placement Probabilities",
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
            subtitle={`Starts: ${row.starts}`}
            metrics={[
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

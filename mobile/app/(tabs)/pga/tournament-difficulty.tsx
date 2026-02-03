import { ScrollView, View, Text, TextInput, ActivityIndicator } from "react-native";
import { Stack } from "expo-router";
import { useMemo, useState } from "react";

import { useTheme } from "@/store/useTheme";
import { usePgaQuery } from "@/hooks/pga/usePgaQuery";
import { MetricCard } from "@/components/pga/MetricCard";
import { PgaTournamentDifficultyRow } from "@/types/pga";

type Response = {
  season: number;
  rows: PgaTournamentDifficultyRow[];
};

export default function TournamentDifficultyScreen() {
  const { colors } = useTheme();
  const [season, setSeason] = useState(String(new Date().getFullYear()));
  const seasonNum = useMemo(() => Number(season) || undefined, [season]);

  const { data, loading, error } = usePgaQuery<Response>(
    "/pga/analytics/tournament-difficulty",
    { season: seasonNum }
  );

  return (
    <>
      <Stack.Screen
        options={{
          title: "Tournament Difficulty",
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
            key={row.tournament_id}
            title={row.tournament?.name ?? "Tournament"}
            subtitle={row.tournament?.course_name ?? ""}
            metrics={[
              { label: "Scoring Avg", value: row.scoring_average },
              { label: "Scoring Diff", value: row.scoring_diff },
              { label: "Difficulty", value: row.difficulty_rank },
              { label: "Birdie Rate", value: row.birdie_rate },
              { label: "Bogey Rate", value: row.bogey_rate },
            ]}
          />
        ))}
      </ScrollView>
    </>
  );
}

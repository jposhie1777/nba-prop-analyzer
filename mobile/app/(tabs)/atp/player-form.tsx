import { ScrollView, View, Text, TextInput, ActivityIndicator } from "react-native";
import { Stack } from "expo-router";
import { useMemo, useState } from "react";

import { useTheme } from "@/store/useTheme";
import { useAtpQuery } from "@/hooks/atp/useAtpQuery";
import { MetricCard } from "@/components/pga/MetricCard";
import { AtpPlayerFormRow } from "@/types/atp";

type Response = {
  seasons: number[];
  rows: AtpPlayerFormRow[];
};

export default function AtpPlayerFormScreen() {
  const { colors } = useTheme();
  const [season, setSeason] = useState(String(new Date().getFullYear()));
  const [surface, setSurface] = useState("");
  const seasonNum = useMemo(() => Number(season) || undefined, [season]);

  const { data, loading, error } = useAtpQuery<Response>(
    "/atp/analytics/player-form",
    {
      season: seasonNum,
      last_n: 12,
      min_matches: 5,
      surface: surface.trim() || undefined,
    }
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

        <Text style={{ color: colors.text.primary, fontWeight: "700", marginTop: 12 }}>
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

        <Text style={{ color: colors.text.muted, marginTop: 6, fontSize: 12 }}>
          Based on last 12 matches for the selected season.
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
            title={row.player.full_name || `${row.player.first_name ?? ""} ${row.player.last_name ?? ""}`}
            subtitle={`Matches: ${row.matches} | Recent: ${row.recent_results.join(", ")}`}
            metrics={[
              { label: "Win Rate", value: row.win_rate },
              { label: "Straight Sets", value: row.straight_sets_rate },
              { label: "Avg Sets", value: row.avg_sets },
              { label: "Tiebreak Rate", value: row.tiebreak_rate },
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

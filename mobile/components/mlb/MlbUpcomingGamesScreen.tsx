import { ActivityIndicator, Pressable, ScrollView, StyleSheet, Text, View } from "react-native";
import { useRouter } from "expo-router";

import { useMlbUpcomingGames } from "@/hooks/mlb/useMlbMatchups";
import { useTheme } from "@/store/useTheme";
import { formatET } from "@/lib/time/formatET";

function scoreColor(grade?: string | null) {
  const normalized = (grade || "").toUpperCase();
  if (normalized === "IDEAL") return "#22C55E";
  if (normalized === "FAVORABLE") return "#10B981";
  if (normalized === "AVERAGE") return "#F59E0B";
  if (normalized === "AVOID") return "#EF4444";
  return "#64748B";
}

export function MlbUpcomingGamesScreen() {
  const router = useRouter();
  const { colors } = useTheme();
  const { data, loading, error, refetch } = useMlbUpcomingGames();

  return (
    <ScrollView style={styles.screen} contentContainerStyle={styles.content}>
      <View style={[styles.hero, { borderColor: colors.border.subtle }]}>
        <Text style={styles.eyebrow}>MLB MATCHUP ANALYTICS</Text>
        <Text style={styles.h1}>Upcoming Games</Text>
        <Text style={styles.sub}>
          Today&apos;s MLB games with start time and model status. Tap any game to open matchup details.
        </Text>
      </View>

      {loading ? <ActivityIndicator color="#22D3EE" /> : null}

      {error ? (
        <Pressable onPress={refetch} style={[styles.errorBox, { borderColor: colors.border.subtle }]}>
          <Text style={styles.errorTitle}>Failed to load MLB games.</Text>
          <Text style={styles.errorText}>{error}</Text>
          <Text style={styles.errorRetry}>Tap to retry</Text>
        </Pressable>
      ) : null}

      {(data ?? []).map((game) => {
        const gamePk = game.game_pk;
        const away = game.away_team ?? "Away";
        const home = game.home_team ?? "Home";
        const topGrade = game.top_grade ?? "Pending";
        const topColor = scoreColor(topGrade);
        const topScore = game.top_score != null ? game.top_score.toFixed(1) : "—";
        const hasData = Boolean(game.has_model_data);

        return (
          <Pressable
            key={String(gamePk)}
            onPress={() =>
              router.push({
                pathname: "/(tabs)/mlb/match/[gamePk]",
                params: {
                  gamePk: String(gamePk),
                  homeTeam: home,
                  awayTeam: away,
                  startTimeUtc: game.start_time_utc ?? "",
                },
              })
            }
            style={[styles.card, { borderColor: colors.border.subtle }]}
          >
            <View style={styles.cardTop}>
              <Text style={[styles.matchup, { color: colors.text.primary }]} numberOfLines={1}>
                {away} @ {home}
              </Text>
              <Text style={[styles.time, { color: "#C7D2FE" }]}>{formatET(game.start_time_utc ?? null)} ET</Text>
            </View>

            <Text style={[styles.meta, { color: colors.text.muted }]}>
              {game.venue_name ? `${game.venue_name} • ` : ""}
              {hasData ? `${game.picks_count ?? 0} picks` : "Model data pending"}
            </Text>

            <View style={styles.tagRow}>
              <View style={[styles.tag, { borderColor: hasData ? "#22C55E55" : "#334155" }]}>
                <Text style={[styles.tagText, { color: hasData ? "#86EFAC" : "#94A3B8" }]}>
                  {hasData ? "Model Ready" : "Pending"}
                </Text>
              </View>
              <View style={[styles.tag, { borderColor: `${topColor}66` }]}>
                <Text style={[styles.tagText, { color: topColor }]}>
                  Top {topGrade} • {topScore}
                </Text>
              </View>
            </View>
          </Pressable>
        );
      })}

      {!loading && !error && (data ?? []).length === 0 ? (
        <View style={[styles.card, { borderColor: colors.border.subtle }]}>
          <Text style={[styles.matchup, { color: colors.text.primary }]}>No MLB games found today.</Text>
          <Text style={[styles.meta, { color: colors.text.muted }]}>
            Games will appear here once today&apos;s schedule is available.
          </Text>
        </View>
      ) : null}
    </ScrollView>
  );
}

const styles = StyleSheet.create({
  screen: { flex: 1, backgroundColor: "#050A18" },
  content: { padding: 16, gap: 10, paddingBottom: 40 },
  hero: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 16,
    backgroundColor: "#071731",
    padding: 16,
    marginBottom: 8,
  },
  eyebrow: { color: "#90B3E9", fontSize: 11, fontWeight: "700" },
  h1: { color: "#E9F2FF", fontSize: 24, fontWeight: "800", marginTop: 8 },
  sub: { color: "#A7C0E8", marginTop: 6, lineHeight: 18, fontSize: 13 },
  card: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 14,
    backgroundColor: "#0B1529",
    padding: 12,
    gap: 8,
  },
  cardTop: { flexDirection: "row", justifyContent: "space-between", alignItems: "center", gap: 10 },
  matchup: { fontSize: 14, fontWeight: "800", flex: 1 },
  time: { fontSize: 12, fontWeight: "700" },
  meta: { fontSize: 12 },
  tagRow: { flexDirection: "row", gap: 8, flexWrap: "wrap" },
  tag: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 999,
    paddingHorizontal: 10,
    paddingVertical: 5,
    backgroundColor: "#0F172A",
  },
  tagText: { fontSize: 11, fontWeight: "700" },
  errorBox: { borderWidth: StyleSheet.hairlineWidth, borderRadius: 12, backgroundColor: "#1F2937", padding: 12 },
  errorTitle: { color: "#FCA5A5", fontWeight: "700" },
  errorText: { color: "#FECACA", marginTop: 4, fontSize: 12 },
  errorRetry: { color: "#E5E7EB", marginTop: 8, fontSize: 12 },
});

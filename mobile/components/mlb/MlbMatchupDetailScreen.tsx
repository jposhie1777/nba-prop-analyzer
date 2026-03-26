import { ActivityIndicator, Pressable, ScrollView, StyleSheet, Text, View } from "react-native";
import { useLocalSearchParams, useRouter } from "expo-router";

import { useMlbMatchupDetail } from "@/hooks/mlb/useMlbMatchups";
import { useTheme } from "@/store/useTheme";

type StatValue = number | string | null | undefined;

function formatScore(score?: number | null): string {
  if (score == null || Number.isNaN(score)) return "—";
  return score.toFixed(1);
}

function formatMetric(value: StatValue, digits = 1): string {
  if (value == null) return "—";
  if (typeof value === "string") return value;
  if (!Number.isFinite(value)) return "—";
  return Number.isInteger(value) ? `${value}` : value.toFixed(digits);
}

function normalizeGrade(grade?: string | null): "IDEAL" | "FAVORABLE" | "AVERAGE" | "AVOID" {
  const g = (grade ?? "").toUpperCase();
  if (g === "IDEAL") return "IDEAL";
  if (g === "FAVORABLE") return "FAVORABLE";
  if (g === "AVOID") return "AVOID";
  return "AVERAGE";
}

function gradeTone(grade?: string | null) {
  const g = normalizeGrade(grade);
  if (g === "IDEAL") return { border: "#10B981", bg: "rgba(16,185,129,0.12)", text: "#A7F3D0" };
  if (g === "FAVORABLE") return { border: "#22D3EE", bg: "rgba(34,211,238,0.12)", text: "#CFFAFE" };
  if (g === "AVOID") return { border: "#EF4444", bg: "rgba(239,68,68,0.12)", text: "#FECACA" };
  return { border: "#F59E0B", bg: "rgba(245,158,11,0.12)", text: "#FDE68A" };
}

export function MlbMatchupDetailScreen() {
  const router = useRouter();
  const { colors } = useTheme();
  const params = useLocalSearchParams<{ gamePk?: string; awayTeam?: string; homeTeam?: string }>();
  const gamePk = Number(params.gamePk);
  const { data, loading, error, refetch } = useMlbMatchupDetail(Number.isFinite(gamePk) ? gamePk : null);

  const game = data?.game;
  const awayTeam = params.awayTeam ?? game?.away_team ?? "Away";
  const homeTeam = params.homeTeam ?? game?.home_team ?? "Home";
  const topCount = (data?.grade_counts?.IDEAL ?? 0) + (data?.grade_counts?.FAVORABLE ?? 0);

  return (
    <ScrollView style={styles.screen} contentContainerStyle={styles.content}>
      <View style={styles.actionRow}>
        <Pressable onPress={() => router.back()} style={[styles.actionButton, { borderColor: colors.border.subtle }]}>
          <Text style={styles.actionText}>← Back</Text>
        </Pressable>
      </View>

      <View style={[styles.hero, { borderColor: colors.border.subtle }]}>
        <Text style={styles.eyebrow}>DAILY TOP PICKS ANALYTICS - HOME RUN REPORT</Text>
        <Text style={styles.title}>{awayTeam} @ {homeTeam}</Text>
        <Text style={styles.sub}>
          {game?.venue_name ?? "Venue TBD"} • {game?.start_time_utc ? new Date(game.start_time_utc).toLocaleString() : "Start TBD"} • HR Matchup Analysis
        </Text>
        <View style={styles.tagRow}>
          <View style={styles.pill}><Text style={styles.pillText}>✨ {topCount} Top Picks</Text></View>
          <View style={styles.pill}><Text style={styles.pillText}>☁ Weather: soon</Text></View>
          <View style={styles.pill}><Text style={styles.pillText}>🏟 Park: soon</Text></View>
          <View style={styles.pill}><Text style={styles.pillText}>💰 Odds: soon</Text></View>
        </View>
      </View>

      {loading ? <ActivityIndicator color="#93C5FD" /> : null}

      {error ? (
        <Pressable onPress={refetch} style={[styles.errorBox, { borderColor: colors.border.subtle }]}>
          <Text style={styles.errorTitle}>Failed to load MLB matchup.</Text>
          <Text style={styles.errorText}>{error}</Text>
          <Text style={styles.errorRetry}>Tap to retry</Text>
        </Pressable>
      ) : null}

      {(data?.pitchers ?? []).map((pitcher) => {
        const season = pitcher.splits?.Season;
        const vsL = pitcher.splits?.vsLHB;
        const vsR = pitcher.splits?.vsRHB;
        return (
          <View key={String(pitcher.pitcher_id)} style={[styles.panel, { borderColor: colors.border.subtle }]}>
            <Text style={styles.sectionEyebrow}>SIDE 1 - {pitcher.offense_team ?? "OFFENSE"}</Text>
            <Text style={styles.sectionTitle}>
              {(pitcher.offense_team ?? "Batters")} vs {pitcher.pitcher_name ?? "Pitcher"}
            </Text>
            <Text style={styles.sectionSub}>
              {pitcher.pitcher_hand ?? "RHP"} • Season HR/9 {formatMetric(season?.hr_per_9, 2)} • Barrel% {formatMetric(season?.barrel_pct)} • HR/FB% {formatMetric(season?.hr_fb_pct)}
            </Text>

            <View style={styles.pitcherCard}>
              <Text style={styles.pitcherName}>{pitcher.pitcher_name ?? "Pitcher"}</Text>
              <View style={styles.statTable}>
                <Text style={styles.headerCell}>SPLIT</Text>
                <Text style={styles.headerCell}>IP</Text>
                <Text style={styles.headerCell}>HR/9</Text>
                <Text style={styles.headerCell}>BARREL%</Text>
                <Text style={styles.headerCell}>HARDHIT%</Text>
                <Text style={styles.headerCell}>FB%</Text>
                <Text style={styles.headerCell}>HR/FB%</Text>

                <Text style={styles.rowLabel}>SEASON</Text>
                <Text style={styles.rowCell}>{formatMetric(season?.ip)}</Text>
                <Text style={styles.rowCell}>{formatMetric(season?.hr_per_9, 2)}</Text>
                <Text style={styles.rowCell}>{formatMetric(season?.barrel_pct)}</Text>
                <Text style={styles.rowCell}>{formatMetric(season?.hard_hit_pct)}</Text>
                <Text style={styles.rowCell}>{formatMetric(season?.fb_pct)}</Text>
                <Text style={styles.rowCell}>{formatMetric(season?.hr_fb_pct)}</Text>

                <Text style={styles.rowLabel}>VS LHB</Text>
                <Text style={styles.rowCell}>{formatMetric(vsL?.ip)}</Text>
                <Text style={styles.rowCell}>{formatMetric(vsL?.hr_per_9, 2)}</Text>
                <Text style={styles.rowCell}>{formatMetric(vsL?.barrel_pct)}</Text>
                <Text style={styles.rowCell}>{formatMetric(vsL?.hard_hit_pct)}</Text>
                <Text style={styles.rowCell}>{formatMetric(vsL?.fb_pct)}</Text>
                <Text style={styles.rowCell}>{formatMetric(vsL?.hr_fb_pct)}</Text>

                <Text style={styles.rowLabel}>VS RHB</Text>
                <Text style={styles.rowCell}>{formatMetric(vsR?.ip)}</Text>
                <Text style={styles.rowCell}>{formatMetric(vsR?.hr_per_9, 2)}</Text>
                <Text style={styles.rowCell}>{formatMetric(vsR?.barrel_pct)}</Text>
                <Text style={styles.rowCell}>{formatMetric(vsR?.hard_hit_pct)}</Text>
                <Text style={styles.rowCell}>{formatMetric(vsR?.fb_pct)}</Text>
                <Text style={styles.rowCell}>{formatMetric(vsR?.hr_fb_pct)}</Text>
              </View>
            </View>

            {(pitcher.batters ?? []).map((batter) => {
              const tone = gradeTone(batter.grade);
              return (
                <View
                  key={`${pitcher.pitcher_id}-${batter.batter_id ?? batter.batter_name}`}
                  style={[styles.batterCard, { borderColor: tone.border, backgroundColor: tone.bg }]}
                >
                  <View style={styles.batterHead}>
                    <Text style={styles.batterName}>{batter.batter_name ?? "Batter"}</Text>
                    <View style={[styles.gradePill, { borderColor: tone.border }]}>
                      <Text style={[styles.gradeText, { color: tone.text }]}>{normalizeGrade(batter.grade)} • {formatScore(batter.score)}</Text>
                    </View>
                  </View>

                  <View style={styles.metricsRow}>
                    <Text style={styles.metric}>ISO {formatMetric(batter.iso, 3)}</Text>
                    <Text style={styles.metric}>SLG {formatMetric(batter.slg, 3)}</Text>
                    <Text style={styles.metric}>L15 EV {formatMetric(batter.l15_ev)}</Text>
                    <Text style={styles.metric}>L15 Barrel {formatMetric(batter.l15_barrel_pct)}%</Text>
                    <Text style={styles.metric}>25 EV {formatMetric(batter.season_ev)}</Text>
                    <Text style={styles.metric}>25 Barrel {formatMetric(batter.season_barrel_pct)}%</Text>
                  </View>

                  {batter.why ? <Text style={styles.whyText}>Why: {batter.why}</Text> : null}
                  {(batter.flags ?? []).length ? <Text style={styles.flagsText}>Signals: {(batter.flags ?? []).slice(0, 3).join(" • ")}</Text> : null}
                </View>
              );
            })}
          </View>
        );
      })}

      {!loading && !error && !(data?.pitchers?.length) ? (
        <View style={[styles.panel, { borderColor: colors.border.subtle }]}>
          <Text style={styles.emptyTitle}>No MLB model data for this game yet.</Text>
          <Text style={styles.emptySub}>Once ingest/model writes land in BigQuery, this matchup view will auto-populate.</Text>
        </View>
      ) : null}
    </ScrollView>
  );
}

const styles = StyleSheet.create({
  screen: { flex: 1, backgroundColor: "#050A18" },
  content: { padding: 16, gap: 10, paddingBottom: 40 },
  actionRow: { flexDirection: "row", gap: 8 },
  actionButton: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 12,
    backgroundColor: "#0B1529",
    paddingHorizontal: 12,
    paddingVertical: 8,
  },
  actionText: { color: "#E9F2FF", fontWeight: "700", fontSize: 12 },
  hero: { borderWidth: StyleSheet.hairlineWidth, borderRadius: 16, backgroundColor: "#071731", padding: 16, gap: 8 },
  eyebrow: { color: "#10B981", fontSize: 11, fontWeight: "700" },
  title: { color: "#E9F2FF", fontSize: 28, fontWeight: "800" },
  sub: { color: "#A7C0E8", fontSize: 12 },
  tagRow: { flexDirection: "row", flexWrap: "wrap", gap: 8, marginTop: 4 },
  pill: {
    borderWidth: StyleSheet.hairlineWidth,
    borderColor: "#334155",
    borderRadius: 999,
    backgroundColor: "#0F172A",
    paddingHorizontal: 10,
    paddingVertical: 6,
  },
  pillText: { color: "#BFDBFE", fontSize: 11, fontWeight: "700" },
  panel: { borderWidth: StyleSheet.hairlineWidth, borderRadius: 14, backgroundColor: "#0B1529", padding: 12, gap: 8 },
  sectionEyebrow: { color: "#64748B", fontSize: 11, fontWeight: "700" },
  sectionTitle: { color: "#E5E7EB", fontSize: 18, fontWeight: "800" },
  sectionSub: { color: "#94A3B8", fontSize: 12 },
  pitcherCard: { borderWidth: StyleSheet.hairlineWidth, borderColor: "#374151", borderRadius: 12, backgroundColor: "#111827", padding: 10, gap: 8 },
  pitcherName: { color: "#E5E7EB", fontSize: 15, fontWeight: "800" },
  statTable: { gap: 4 },
  headerCell: { color: "#94A3B8", fontSize: 10, fontWeight: "700" },
  rowLabel: { color: "#CBD5E1", fontSize: 11, fontWeight: "800", marginTop: 4 },
  rowCell: { color: "#E5E7EB", fontSize: 12 },
  batterCard: { borderWidth: 1, borderRadius: 12, padding: 10, gap: 8 },
  batterHead: { flexDirection: "row", alignItems: "center", justifyContent: "space-between", gap: 8 },
  batterName: { color: "#E5E7EB", fontSize: 15, fontWeight: "700", flex: 1 },
  gradePill: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 999,
    backgroundColor: "rgba(15,23,42,0.7)",
    paddingHorizontal: 10,
    paddingVertical: 4,
  },
  gradeText: { fontSize: 11, fontWeight: "800" },
  metricsRow: { flexDirection: "row", flexWrap: "wrap", gap: 10 },
  metric: { color: "#C7D2FE", fontSize: 12, fontWeight: "600" },
  whyText: { color: "#E2E8F0", fontSize: 12, lineHeight: 18 },
  flagsText: { color: "#94A3B8", fontSize: 11, lineHeight: 16 },
  errorBox: { borderWidth: StyleSheet.hairlineWidth, borderRadius: 12, backgroundColor: "#1F2937", padding: 12 },
  errorTitle: { color: "#FCA5A5", fontWeight: "700" },
  errorText: { color: "#FECACA", marginTop: 4, fontSize: 12 },
  errorRetry: { color: "#E5E7EB", marginTop: 8, fontSize: 12 },
  emptyTitle: { color: "#E5E7EB", fontWeight: "700" },
  emptySub: { color: "#A7C0E8", marginTop: 6, fontSize: 12 },
});

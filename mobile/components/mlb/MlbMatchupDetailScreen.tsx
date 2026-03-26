import { ActivityIndicator, Image, Pressable, ScrollView, StyleSheet, Text, View } from "react-native";
import { useLocalSearchParams } from "expo-router";

import { useMlbMatchupDetail } from "@/hooks/mlb/useMlbMatchups";
import { useTheme } from "@/store/useTheme";
import { getMlbTeamLogo } from "@/utils/mlbLogos";
import { BackToHomeButton } from "@/components/navigation/BackToHomeButton";

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

function metricTier(metric: "iso" | "slg" | "l15_ev" | "l15_barrel" | "l25_ev" | "l25_barrel", value?: number | null) {
  if (value == null || Number.isNaN(value)) return "default";
  if (metric === "iso") return value >= 0.2 ? "elite" : "default";
  if (metric === "slg") return value >= 0.5 ? "elite" : "default";
  if (metric === "l15_ev") return value >= 98 ? "elite" : "default";
  if (metric === "l15_barrel") return value >= 20 ? "elite" : "default";
  if (metric === "l25_ev") return value >= 91 ? "elite" : "default";
  if (metric === "l25_barrel") return value >= 10 ? "elite" : "default";
  return "default";
}

function metricTone(metric: "iso" | "slg" | "l15_ev" | "l15_barrel" | "l25_ev" | "l25_barrel", value?: number | null) {
  return metricTier(metric, value) === "elite" ? styles.metricElite : styles.metricDefault;
}

function StickyPitcherCard({
  pitcher,
}: {
  pitcher: {
    pitcher_name?: string | null;
    pitcher_hand?: string | null;
    offense_team?: string | null;
    splits: Record<string, any>;
  };
}) {
  const season = pitcher.splits?.Season;
  const vsL = pitcher.splits?.vsLHB;
  const vsR = pitcher.splits?.vsRHB;
  return (
    <View style={styles.stickyPitcherWrap}>
      <Text style={styles.stickyTitle}>
        {pitcher.pitcher_name ?? "Pitcher"} ({pitcher.pitcher_hand ?? "RHP"}) - {pitcher.offense_team ?? "Offense"}
      </Text>
      <View style={styles.tableWrap}>
        <View style={[styles.tableRow, styles.tableHeaderRow]}>
          <Text style={[styles.headerCell, styles.cellSplit]}>SPLIT</Text>
          <Text style={styles.headerCell}>IP</Text>
          <Text style={styles.headerCell}>HR</Text>
          <Text style={styles.headerCell}>HR/9</Text>
          <Text style={styles.headerCell}>BARREL%</Text>
          <Text style={styles.headerCell}>HARDHIT%</Text>
          <Text style={styles.headerCell}>FB%</Text>
          <Text style={styles.headerCell}>HR/FB%</Text>
          <Text style={styles.headerCell}>WHIP</Text>
        </View>
        <SplitRow label="SEASON" split={season} />
        <SplitRow label="VS LHB" split={vsL} />
        <SplitRow label="VS RHB" split={vsR} />
      </View>
    </View>
  );
}

function dedupeBatters<T extends { batter_id?: number | null; batter_name?: string | null; score?: number | null }>(
  batters: T[]
): T[] {
  const byKey = new Map<string, T>();
  for (const batter of batters) {
    const key = batter.batter_id != null ? `id:${batter.batter_id}` : `name:${(batter.batter_name ?? "").toLowerCase()}`;
    const existing = byKey.get(key);
    if (!existing || (batter.score ?? 0) > (existing.score ?? 0)) {
      byKey.set(key, batter);
    }
  }
  return Array.from(byKey.values()).sort((a, b) => (b.score ?? 0) - (a.score ?? 0));
}

function SplitRow({
  label,
  split,
}: {
  label: string;
  split?: {
    ip?: number | null;
    home_runs?: number | null;
    hr_per_9?: number | null;
    barrel_pct?: number | null;
    hard_hit_pct?: number | null;
    fb_pct?: number | null;
    hr_fb_pct?: number | null;
    whip?: number | null;
  };
}) {
  return (
    <View style={styles.tableRow}>
      <Text style={[styles.cell, styles.cellSplit]}>{label}</Text>
      <Text style={styles.cell}>{formatMetric(split?.ip, 1)}</Text>
      <Text style={styles.cell}>{formatMetric(split?.home_runs, 0)}</Text>
      <Text style={styles.cell}>{formatMetric(split?.hr_per_9, 2)}</Text>
      <Text style={styles.cell}>{formatMetric(split?.barrel_pct, 1)}</Text>
      <Text style={styles.cell}>{formatMetric(split?.hard_hit_pct, 1)}</Text>
      <Text style={styles.cell}>{formatMetric(split?.fb_pct, 1)}</Text>
      <Text style={styles.cell}>{formatMetric(split?.hr_fb_pct, 1)}</Text>
      <Text style={styles.cell}>{formatMetric(split?.whip, 2)}</Text>
    </View>
  );
}

export function MlbMatchupDetailScreen() {
  const { colors } = useTheme();
  const params = useLocalSearchParams<{ gamePk?: string; awayTeam?: string; homeTeam?: string }>();
  const gamePk = Number(params.gamePk);
  const { data, loading, error, refetch } = useMlbMatchupDetail(Number.isFinite(gamePk) ? gamePk : null);

  const game = data?.game;
  const awayTeam = params.awayTeam ?? game?.away_team ?? "Away";
  const homeTeam = params.homeTeam ?? game?.home_team ?? "Home";
  const topCount = (data?.grade_counts?.IDEAL ?? 0) + (data?.grade_counts?.FAVORABLE ?? 0);
  const awayLogo = getMlbTeamLogo(awayTeam);
  const homeLogo = getMlbTeamLogo(homeTeam);
  return (
    <ScrollView style={styles.screen} contentContainerStyle={styles.content}>
      <View style={styles.actionRow}>
        <BackToHomeButton />
      </View>

      <View style={[styles.hero, { borderColor: colors.border.subtle }]}>
        <Text style={styles.eyebrow}>MLB MATCHUP ANALYTICS</Text>
        <View style={styles.slugRow}>
          <View style={styles.heroTeamCol}>
            {awayLogo ? <Image source={{ uri: awayLogo }} style={styles.heroLogo} /> : <View style={styles.heroLogo} />}
            <Text style={styles.heroTeamName} numberOfLines={2}>{awayTeam}</Text>
          </View>
          <View style={styles.heroCenterCol}>
            <Text style={styles.slugTime}>{game?.start_time_utc ? new Date(game.start_time_utc).toLocaleTimeString() : "TBD"}</Text>
            <Text style={styles.slugMeta}>{game?.venue_name ?? "Venue TBD"}</Text>
            <Text style={styles.slugTop}>Top Rated Batter • {topCount} Favorable+</Text>
          </View>
          <View style={styles.heroTeamCol}>
            {homeLogo ? <Image source={{ uri: homeLogo }} style={styles.heroLogo} /> : <View style={styles.heroLogo} />}
            <Text style={styles.heroTeamName} numberOfLines={2}>{homeTeam}</Text>
          </View>
        </View>
        <View style={styles.tagRow}>
          <View style={styles.pill}><Text style={styles.pillText}>☁ Weather: soon</Text></View>
          <View style={styles.pill}><Text style={styles.pillText}>🏟 Park: soon</Text></View>
          <View style={styles.pill}><Text style={styles.pillText}>💰 Odds: soon</Text></View>
        </View>
      </View>

      {(data?.pitchers ?? []).map((pitcher) => {
        const season = pitcher.splits?.Season;
        const vsL = pitcher.splits?.vsLHB;
        const vsR = pitcher.splits?.vsRHB;
        return (
          <View key={`sticky-${String(pitcher.pitcher_id)}`} style={styles.stickyPitcherWrap}>
            <Text style={styles.stickyTitle}>
              {pitcher.pitcher_name ?? "Pitcher"} ({pitcher.pitcher_hand ?? "RHP"}) • {pitcher.offense_team ?? "Offense"}
            </Text>
            <View style={styles.tableWrap}>
              <View style={[styles.tableRow, styles.tableHeaderRow]}>
                <Text style={[styles.headerCell, styles.cellSplit]}>SPLIT</Text>
                <Text style={styles.headerCell}>IP</Text>
                <Text style={styles.headerCell}>HR</Text>
                <Text style={styles.headerCell}>HR/9</Text>
                <Text style={styles.headerCell}>BARREL%</Text>
                <Text style={styles.headerCell}>HARDHIT%</Text>
                <Text style={styles.headerCell}>FB%</Text>
                <Text style={styles.headerCell}>HR/FB%</Text>
                <Text style={styles.headerCell}>WHIP</Text>
              </View>
              <SplitRow label="SEASON" split={season} />
              <SplitRow label="VS LHB" split={vsL} />
              <SplitRow label="VS RHB" split={vsR} />
            </View>
          </View>
        );
      })}

      {loading ? <ActivityIndicator color="#93C5FD" /> : null}

      {error ? (
        <Pressable onPress={refetch} style={[styles.errorBox, { borderColor: colors.border.subtle }]}>
          <Text style={styles.errorTitle}>Failed to load MLB matchup.</Text>
          <Text style={styles.errorText}>{error}</Text>
          <Text style={styles.errorRetry}>Tap to retry</Text>
        </Pressable>
      ) : null}

      {(data?.pitchers ?? []).map((pitcher, pitcherIdx) => (
        <View key={`sticky-web-${String(pitcher.pitcher_id)}`} style={styles.stickySlot}>
          <StickyPitcherCard pitcher={pitcher as any} />
        </View>
      ))}

      {(data?.pitchers ?? []).map((pitcher, pitcherIdx) => {
        const season = pitcher.splits?.Season;
        const vsL = pitcher.splits?.vsLHB;
        const vsR = pitcher.splits?.vsRHB;
        const batters = dedupeBatters(pitcher.batters ?? []).slice(0, 12);
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
              <View style={styles.tableWrap}>
                <View style={[styles.tableRow, styles.tableHeaderRow]}>
                  <Text style={[styles.headerCell, styles.cellSplit]}>SPLIT</Text>
                  <Text style={styles.headerCell}>IP</Text>
                  <Text style={styles.headerCell}>HR</Text>
                  <Text style={styles.headerCell}>HR/9</Text>
                  <Text style={styles.headerCell}>BARREL%</Text>
                  <Text style={styles.headerCell}>HARDHIT%</Text>
                  <Text style={styles.headerCell}>FB%</Text>
                  <Text style={styles.headerCell}>HR/FB%</Text>
                  <Text style={styles.headerCell}>WHIP</Text>
                </View>
                <SplitRow label="SEASON" split={season} />
                <SplitRow label="VS LHB" split={vsL} />
                <SplitRow label="VS RHB" split={vsR} />
              </View>
            </View>

            {!batters.length ? (
              <Text style={styles.emptySub}>No hitter rows returned for this pitcher.</Text>
            ) : null}
            {batters.map((batter) => {
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

                  <View style={styles.metricsTable}>
                    <View style={styles.metricsHeaderRow}>
                      <Text style={styles.metricsHeaderCell}>ISO</Text>
                      <Text style={styles.metricsHeaderCell}>SLG</Text>
                      <Text style={styles.metricsHeaderCell}>L15 EV</Text>
                      <Text style={styles.metricsHeaderCell}>L15 Barrel%</Text>
                      <Text style={styles.metricsHeaderCell}>L25 EV</Text>
                      <Text style={styles.metricsHeaderCell}>L25 Barrel%</Text>
                    </View>
                    <View style={styles.metricsValueRow}>
                      <Text style={[styles.metricsValueCell, metricTone("iso", batter.iso)]}>{formatMetric(batter.iso, 3)}</Text>
                      <Text style={[styles.metricsValueCell, metricTone("slg", batter.slg)]}>{formatMetric(batter.slg, 3)}</Text>
                      <Text style={[styles.metricsValueCell, metricTone("l15_ev", batter.l15_ev)]}>{formatMetric(batter.l15_ev, 1)}</Text>
                      <Text style={[styles.metricsValueCell, metricTone("l15_barrel", batter.l15_barrel_pct)]}>{formatMetric(batter.l15_barrel_pct, 1)}</Text>
                      <Text style={[styles.metricsValueCell, metricTone("l25_ev", batter.season_ev)]}>{formatMetric(batter.season_ev, 1)}</Text>
                      <Text style={[styles.metricsValueCell, metricTone("l25_barrel", batter.season_barrel_pct)]}>{formatMetric(batter.season_barrel_pct, 1)}</Text>
                    </View>
                  </View>

                  {batter.why ? <Text style={styles.whyText}>Why: {batter.why}</Text> : null}
                  {(batter.flags ?? []).length ? <Text style={styles.flagsText}>Signals: {(batter.flags ?? []).slice(0, 4).join(" • ")}</Text> : null}
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
  actionRow: { flexDirection: "row", gap: 8, marginBottom: 2 },
  hero: { borderWidth: StyleSheet.hairlineWidth, borderRadius: 16, backgroundColor: "#071731", padding: 16, gap: 8 },
  eyebrow: { color: "#10B981", fontSize: 11, fontWeight: "700" },
  slugRow: { flexDirection: "row", alignItems: "center", justifyContent: "space-between", gap: 12 },
  heroTeamCol: { flex: 1.25, alignItems: "center", gap: 6 },
  heroCenterCol: { flex: 1.8, alignItems: "center", gap: 3 },
  heroLogo: { width: 36, height: 36, borderRadius: 18, backgroundColor: "#111827" },
  heroTeamName: { color: "#E5E7EB", fontSize: 14, fontWeight: "800", textAlign: "center" },
  slugTime: { color: "#F8FAFC", fontSize: 18, fontWeight: "800" },
  slugMeta: { color: "#A7C0E8", fontSize: 11 },
  slugTop: { color: "#34D399", fontSize: 11, fontWeight: "700" },
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
  stickyPitcherWrap: {
    position: "sticky" as any,
    top: 78,
    zIndex: 50,
    borderWidth: StyleSheet.hairlineWidth,
    borderColor: "#1F2937",
    borderRadius: 12,
    backgroundColor: "rgba(5,10,24,0.97)",
    padding: 10,
    gap: 6,
  },
  stickySlot: { marginBottom: 0 },
  stickyTitle: { color: "#D1D5DB", fontSize: 12, fontWeight: "800" },
  sectionEyebrow: { color: "#64748B", fontSize: 11, fontWeight: "700" },
  sectionTitle: { color: "#E5E7EB", fontSize: 18, fontWeight: "800" },
  sectionSub: { color: "#94A3B8", fontSize: 12 },
  pitcherCard: { borderWidth: StyleSheet.hairlineWidth, borderColor: "#374151", borderRadius: 12, backgroundColor: "#111827", padding: 10, gap: 8 },
  pitcherName: { color: "#E5E7EB", fontSize: 15, fontWeight: "800" },
  tableWrap: { gap: 2 },
  tableHeaderRow: { borderBottomWidth: StyleSheet.hairlineWidth, borderBottomColor: "#334155", paddingBottom: 4, marginBottom: 2 },
  tableRow: { flexDirection: "row", alignItems: "center" },
  headerCell: { color: "#94A3B8", fontSize: 10, fontWeight: "800", flex: 1, textAlign: "center" },
  cell: { color: "#E5E7EB", fontSize: 11, flex: 1, textAlign: "center", paddingVertical: 3 },
  cellSplit: { flex: 1.5, textAlign: "left", fontWeight: "800", color: "#CBD5E1" },
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
  metricsTable: {
    borderWidth: StyleSheet.hairlineWidth,
    borderColor: "#334155",
    borderRadius: 8,
    overflow: "hidden",
  },
  metricsHeaderRow: { flexDirection: "row", backgroundColor: "#0F172A" },
  metricsHeaderCell: { flex: 1, color: "#94A3B8", fontSize: 10, fontWeight: "700", textAlign: "center", paddingVertical: 6 },
  metricsValueRow: { flexDirection: "row", backgroundColor: "rgba(15,23,42,0.25)" },
  metricsValueCell: { flex: 1, fontSize: 12, fontWeight: "700", textAlign: "center", paddingVertical: 8 },
  metricDefault: { color: "#E2E8F0" },
  metricElite: { color: "#34D399" },
  whyText: { color: "#E2E8F0", fontSize: 12, lineHeight: 18 },
  flagsText: { color: "#94A3B8", fontSize: 11, lineHeight: 16 },
  errorBox: { borderWidth: StyleSheet.hairlineWidth, borderRadius: 12, backgroundColor: "#1F2937", padding: 12 },
  errorTitle: { color: "#FCA5A5", fontWeight: "700" },
  errorText: { color: "#FECACA", marginTop: 4, fontSize: 12 },
  errorRetry: { color: "#E5E7EB", marginTop: 8, fontSize: 12 },
  emptyTitle: { color: "#E5E7EB", fontWeight: "700" },
  emptySub: { color: "#A7C0E8", marginTop: 6, fontSize: 12 },
});

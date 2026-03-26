import { useState } from "react";
import { ActivityIndicator, Image, Linking, Platform, Pressable, ScrollView, StyleSheet, Text, View } from "react-native";
import { useLocalSearchParams } from "expo-router";

import { useMlbMatchupDetail, MlbBatterPick, MlbPitcherGroup } from "@/hooks/mlb/useMlbMatchups";
import { useTheme } from "@/store/useTheme";
import { getMlbTeamLogo } from "@/utils/mlbLogos";
import { BackToHomeButton } from "@/components/navigation/BackToHomeButton";
import { BetslipDrawer } from "@/components/live/BetslipDrawer";
import { BetslipToggle } from "@/components/live/BetslipToggle";

// ─── Helpers ─────────────────────────────────────────────────────────────────

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

function formatOdds(ml?: number | null): string {
  if (ml == null) return "—";
  return ml > 0 ? `+${ml}` : `${ml}`;
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

function windArrow(windDir?: number | null, azimuth?: number | null): { rotation: number; color: string } | null {
  if (windDir == null || azimuth == null) return null;
  const rel = ((windDir - azimuth) + 360) % 360;
  // Arrow points in the direction wind is blowing TO (rel+180), clockwise from "toward CF"
  const rotation = (rel + 180) % 360;
  // Color by whether wind is helping (blowing out) or hurting (blowing in)
  const color =
    (rel >= 135 && rel <= 225) ? "#86EFAC" :  // blowing out → green
    (rel <= 45 || rel >= 315)  ? "#FCA5A5" :  // blowing in  → red
    "#FDE68A";                                  // crosswind   → yellow
  return { rotation, color };
}

function weatherColor(indicator?: string | null) {
  const w = (indicator || "").toLowerCase();
  if (w === "green") return { border: "#22C55E55", text: "#86EFAC", bg: "rgba(34,197,94,0.08)" };
  if (w === "yellow") return { border: "#F59E0B55", text: "#FDE68A", bg: "rgba(245,158,11,0.08)" };
  if (w === "red") return { border: "#EF444455", text: "#FCA5A5", bg: "rgba(239,68,68,0.08)" };
  return { border: "#33415555", text: "#94A3B8", bg: "transparent" };
}

// ─── Sub-components ───────────────────────────────────────────────────────────

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

function PitcherSplitsTable({ pitcher }: { pitcher: MlbPitcherGroup }) {
  const season = pitcher.splits?.Season;
  const vsL = pitcher.splits?.vsLHB;
  const vsR = pitcher.splits?.vsRHB;
  return (
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

// ─── BatterCard ───────────────────────────────────────────────────────────────

function BatterCard({
  batter,
  gamePk,
  pitcherName,
  offenseTeam,
}: {
  batter: MlbBatterPick;
  gamePk: number;
  pitcherName?: string | null;
  offenseTeam?: string | null;
}) {
  const tone = gradeTone(batter.grade);
  const hasHrOdds = batter.hr_odds_best_price != null;
  const book = (batter.hr_odds_best_book ?? "").trim();
  const oddsLabel = hasHrOdds
    ? (book ? `${formatOdds(batter.hr_odds_best_price)} ${book.slice(0, 10)}` : formatOdds(batter.hr_odds_best_price))
    : null;
  const oddsColor = (batter.hr_odds_best_price ?? 0) >= 0 ? "#86EFAC" : "#FCA5A5";

  function handleOddsPress() {
    const link = Platform.OS === "ios"
      ? batter.deep_link_ios ?? batter.deep_link_desktop
      : batter.deep_link_desktop ?? batter.deep_link_ios;
    if (link) Linking.openURL(link).catch(() => {});
  }

  return (
    <View style={[styles.batterCard, { borderColor: tone.border, backgroundColor: tone.bg }]}>
      {/* ── Header row: name + grade pill + HR odds button ── */}
      <View style={styles.batterHead}>
        <View style={{ flex: 1 }}>
          <Text style={styles.batterName}>{batter.batter_name ?? "Batter"}</Text>
          <Text style={styles.batterSub}>{offenseTeam ?? ""}{batter.bat_side ? ` · ${batter.bat_side === "L" ? "LHB" : "RHB"}` : ""}</Text>
        </View>
        <View style={styles.batterHeadRight}>
          <View style={[styles.gradePill, { borderColor: tone.border }]}>
            <Text style={[styles.gradeText, { color: tone.text }]}>{normalizeGrade(batter.grade)} • {formatScore(batter.score)}</Text>
          </View>
          {oddsLabel ? (
            <Pressable
              onPress={handleOddsPress}
              style={[styles.saveButton, { borderColor: oddsColor, backgroundColor: "transparent" }]}
            >
              <Text style={[styles.saveButtonText, { color: oddsColor }]}>{oddsLabel}</Text>
            </Pressable>
          ) : null}
        </View>
      </View>

      {/* ── Metrics table ── */}
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
}

// ─── PitcherSection ───────────────────────────────────────────────────────────
// Each pitcher section has its OWN sticky header, so the correct pitcher sticks
// at the top as you scroll through that team's batters.

function PitcherSection({
  pitcher,
  gamePk,
  sectionIndex,
}: {
  pitcher: MlbPitcherGroup;
  gamePk: number;
  sectionIndex: number;
}) {
  const batters = dedupeBatters(pitcher.batters ?? []).slice(0, 12);
  const season = pitcher.splits?.Season;

  return (
    <View style={styles.panel}>
      {/* ── Sticky pitcher header – sticks within this section ── */}
      <View style={styles.stickyPitcherInSection}>
        <View style={styles.stickyPitcherTitleRow}>
          <Text style={styles.stickyTitle}>
            {pitcher.pitcher_name ?? "Pitcher"} ({pitcher.pitcher_hand ?? "RHP"})
          </Text>
          <Text style={styles.stickyOffense}>{pitcher.offense_team ?? "Offense"} batting</Text>
        </View>
        <PitcherSplitsTable pitcher={pitcher} />
      </View>

      {/* ── Section label ── */}
      <Text style={styles.sectionEyebrow}>
        SIDE {sectionIndex + 1} — {pitcher.offense_team ?? "OFFENSE"}
      </Text>
      <Text style={styles.sectionSub}>
        {pitcher.pitcher_hand ?? "RHP"} • Season HR/9 {formatMetric(season?.hr_per_9, 2)} • Barrel% {formatMetric(season?.barrel_pct)} • HR/FB% {formatMetric(season?.hr_fb_pct)}
      </Text>

      {/* ── Batters ── */}
      {!batters.length ? (
        <Text style={styles.emptySub}>No hitter rows returned for this pitcher.</Text>
      ) : null}
      {batters.map((batter) => (
        <BatterCard
          key={`${pitcher.pitcher_id}-${batter.batter_id ?? batter.batter_name}`}
          batter={batter}
          gamePk={gamePk}
          pitcherName={pitcher.pitcher_name}
          offenseTeam={pitcher.offense_team}
        />
      ))}
    </View>
  );
}

// ─── Main screen ─────────────────────────────────────────────────────────────

export function MlbMatchupDetailScreen() {
  const { colors } = useTheme();
  const [betslipOpen, setBetslipOpen] = useState(false);
  const params = useLocalSearchParams<{ gamePk?: string; awayTeam?: string; homeTeam?: string }>();
  const gamePk = Number(params.gamePk);
  const { data, loading, error, refetch } = useMlbMatchupDetail(Number.isFinite(gamePk) ? gamePk : null);

  const game = data?.game;
  const awayTeam = params.awayTeam ?? game?.away_team ?? "Away";
  const homeTeam = params.homeTeam ?? game?.home_team ?? "Home";
  const topCount = (data?.grade_counts?.IDEAL ?? 0) + (data?.grade_counts?.FAVORABLE ?? 0);
  const awayLogo = getMlbTeamLogo(awayTeam);
  const homeLogo = getMlbTeamLogo(homeTeam);
  const wc = weatherColor(game?.weather_indicator);

  return (
    <View style={{ flex: 1, backgroundColor: "#050A18" }}>
      <ScrollView style={styles.screen} contentContainerStyle={styles.content}>
        <View style={styles.actionRow}>
          <BackToHomeButton />
        </View>

        {/* ── Hero card ── */}
        <View style={[styles.hero, { borderColor: colors.border.subtle }]}>
          <Text style={styles.eyebrow}>MLB MATCHUP ANALYTICS</Text>
          <View style={styles.slugRow}>
            <View style={styles.heroTeamCol}>
              {awayLogo ? <Image source={{ uri: awayLogo }} style={styles.heroLogo} /> : <View style={styles.heroLogo} />}
              <Text style={styles.heroTeamName} numberOfLines={2}>{awayTeam}</Text>
              {game?.away_moneyline != null ? (
                <Text style={styles.heroOdds}>{formatOdds(game.away_moneyline)}</Text>
              ) : null}
            </View>
            <View style={styles.heroCenterCol}>
              <Text style={styles.slugTime}>
                {game?.start_time_utc ? new Date(game.start_time_utc).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }) : "TBD"}
              </Text>
              <Text style={styles.slugMeta}>{game?.venue_name ?? game?.ballpark_name ?? "Venue TBD"}</Text>
              {game?.over_under != null ? (
                <Text style={styles.slugOU}>O/U {game.over_under}</Text>
              ) : null}
              <Text style={styles.slugTop}>Top Rated • {topCount} Favorable+</Text>
            </View>
            <View style={styles.heroTeamCol}>
              {homeLogo ? <Image source={{ uri: homeLogo }} style={styles.heroLogo} /> : <View style={styles.heroLogo} />}
              <Text style={styles.heroTeamName} numberOfLines={2}>{homeTeam}</Text>
              {game?.home_moneyline != null ? (
                <Text style={styles.heroOdds}>{formatOdds(game.home_moneyline)}</Text>
              ) : null}
            </View>
          </View>

          {/* ── Weather strip ── */}
          {(game?.weather_indicator || game?.game_temp != null || game?.wind_speed != null) ? (
            <View style={[styles.weatherStrip, { borderColor: wc.border, backgroundColor: wc.bg }]}>
              <View style={styles.weatherPills}>
                {game?.weather_indicator ? (
                  <View style={[styles.weatherPill, { borderColor: wc.border }]}>
                    <Text style={[styles.weatherPillText, { color: wc.text }]}>
                      {game.weather_indicator.toLowerCase() === "green" ? "☀" : game.weather_indicator.toLowerCase() === "yellow" ? "⛅" : "🌧"} {game.weather_indicator}
                    </Text>
                  </View>
                ) : null}
                {game?.game_temp != null ? (
                  <View style={styles.weatherPill}>
                    <Text style={styles.weatherPillText}>🌡 {Math.round(game.game_temp)}°F</Text>
                  </View>
                ) : null}
                {game?.wind_speed != null ? (() => {
                  const wa = windArrow(game.wind_dir, game.ballpark_azimuth);
                  return (
                    <View style={styles.weatherPill}>
                      <Text style={styles.weatherPillText}>
                        💨 {game.wind_speed.toFixed(1)} mph{" "}
                        {wa
                          ? <Text style={{ color: wa.color, transform: [{ rotate: `${wa.rotation}deg` }] }}>{"↑"}</Text>
                          : game.wind_dir != null ? `@ ${game.wind_dir}°` : ""}
                      </Text>
                    </View>
                  );
                })() : null}
                {game?.precip_prob != null && game.precip_prob > 0 ? (
                  <View style={styles.weatherPill}>
                    <Text style={styles.weatherPillText}>☔ {Math.round(game.precip_prob)}% precip</Text>
                  </View>
                ) : null}
                {game?.roof_type ? (
                  <View style={styles.weatherPill}>
                    <Text style={styles.weatherPillText}>🏟 {game.roof_type}</Text>
                  </View>
                ) : null}
              </View>
              {game?.conditions ? (
                <Text style={styles.conditionsText}>{game.conditions}</Text>
              ) : null}
              {game?.weather_note ? (
                <Text style={styles.weatherNote}>{game.weather_note}</Text>
              ) : null}
            </View>
          ) : (
            <View style={styles.tagRow}>
              <View style={styles.pill}><Text style={styles.pillText}>Weather data pending</Text></View>
              {game?.ballpark_name ? <View style={styles.pill}><Text style={styles.pillText}>🏟 {game.ballpark_name}</Text></View> : null}
            </View>
          )}
        </View>

        {loading ? <ActivityIndicator color="#93C5FD" /> : null}

        {error ? (
          <Pressable onPress={refetch} style={[styles.errorBox, { borderColor: colors.border.subtle }]}>
            <Text style={styles.errorTitle}>Failed to load MLB matchup.</Text>
            <Text style={styles.errorText}>{error}</Text>
            <Text style={styles.errorRetry}>Tap to retry</Text>
          </Pressable>
        ) : null}

        {/* ── Pitcher sections — each has its own sticky header ── */}
        {(data?.pitchers ?? []).map((pitcher, idx) => (
          <PitcherSection
            key={String(pitcher.pitcher_id)}
            pitcher={pitcher}
            gamePk={gamePk}
            sectionIndex={idx}
          />
        ))}

        {!loading && !error && !(data?.pitchers?.length) ? (
          <View style={[styles.panel, { borderColor: colors.border.subtle }]}>
            <Text style={styles.emptyTitle}>No MLB model data for this game yet.</Text>
            <Text style={styles.emptySub}>Once ingest/model writes land in BigQuery, this matchup view will auto-populate.</Text>
          </View>
        ) : null}

        <View style={{ height: 80 }} />
      </ScrollView>

      {/* ── Betslip overlay ── */}
      <BetslipToggle onPress={() => setBetslipOpen(true)} />
      <BetslipDrawer open={betslipOpen} onClose={() => setBetslipOpen(false)} />
    </View>
  );
}

// ─── Styles ───────────────────────────────────────────────────────────────────

const styles = StyleSheet.create({
  screen: { flex: 1, backgroundColor: "#050A18" },
  content: { padding: 16, gap: 10, paddingBottom: 40 },
  actionRow: { flexDirection: "row", gap: 8, marginBottom: 2 },

  // Hero
  hero: { borderWidth: StyleSheet.hairlineWidth, borderRadius: 16, backgroundColor: "#071731", padding: 16, gap: 8 },
  eyebrow: { color: "#10B981", fontSize: 11, fontWeight: "700" },
  slugRow: { flexDirection: "row", alignItems: "center", justifyContent: "space-between", gap: 12 },
  heroTeamCol: { flex: 1.25, alignItems: "center", gap: 4 },
  heroCenterCol: { flex: 1.8, alignItems: "center", gap: 3 },
  heroLogo: { width: 36, height: 36, borderRadius: 18, backgroundColor: "#111827" },
  heroTeamName: { color: "#E5E7EB", fontSize: 14, fontWeight: "800", textAlign: "center" },
  heroOdds: { color: "#93C5FD", fontSize: 13, fontWeight: "800" },
  slugTime: { color: "#F8FAFC", fontSize: 18, fontWeight: "800" },
  slugMeta: { color: "#A7C0E8", fontSize: 11 },
  slugOU: { color: "#A5B4FC", fontSize: 12, fontWeight: "700" },
  slugTop: { color: "#34D399", fontSize: 11, fontWeight: "700" },

  // Weather strip
  weatherStrip: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 10,
    padding: 10,
    gap: 6,
    marginTop: 4,
  },
  weatherPills: { flexDirection: "row", flexWrap: "wrap", gap: 6 },
  weatherPill: {
    borderWidth: StyleSheet.hairlineWidth,
    borderColor: "#334155",
    borderRadius: 999,
    paddingHorizontal: 8,
    paddingVertical: 4,
    backgroundColor: "#0F172A",
  },
  weatherPillText: { color: "#CBD5E1", fontSize: 11, fontWeight: "700" },
  conditionsText: { color: "#94A3B8", fontSize: 11 },
  weatherNote: { color: "#BFDBFE", fontSize: 11, lineHeight: 16, fontStyle: "italic" },

  // Fallback tag row
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

  // Panel (one per pitcher, contains sticky header + batters)
  panel: {
    borderWidth: StyleSheet.hairlineWidth,
    borderColor: "#1E293B",
    borderRadius: 14,
    backgroundColor: "#0B1529",
    overflow: "hidden",
    gap: 0,
  },

  // Sticky pitcher header — sticks within its own panel section
  stickyPitcherInSection: {
    position: "sticky" as any,
    top: 0,
    zIndex: 30,
    backgroundColor: "rgba(7,14,30,0.97)",
    borderBottomWidth: StyleSheet.hairlineWidth,
    borderBottomColor: "#1F2937",
    padding: 10,
    gap: 6,
  },
  stickyPitcherTitleRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    gap: 8,
  },
  stickyTitle: { color: "#E2E8F0", fontSize: 13, fontWeight: "800", flex: 1 },
  stickyOffense: { color: "#64748B", fontSize: 11, fontWeight: "600" },

  sectionEyebrow: { color: "#64748B", fontSize: 11, fontWeight: "700", paddingHorizontal: 12, paddingTop: 10 },
  sectionSub: { color: "#94A3B8", fontSize: 11, paddingHorizontal: 12, paddingBottom: 6 },

  // Table
  tableWrap: { gap: 2 },
  tableHeaderRow: { borderBottomWidth: StyleSheet.hairlineWidth, borderBottomColor: "#334155", paddingBottom: 4, marginBottom: 2 },
  tableRow: { flexDirection: "row", alignItems: "center" },
  headerCell: { color: "#94A3B8", fontSize: 9, fontWeight: "800", flex: 1, textAlign: "center" },
  cell: { color: "#E5E7EB", fontSize: 10, flex: 1, textAlign: "center", paddingVertical: 2 },
  cellSplit: { flex: 1.5, textAlign: "left", fontWeight: "800", color: "#CBD5E1" },

  // Batter card
  batterCard: { borderWidth: 1, borderRadius: 0, padding: 12, gap: 8, borderLeftWidth: 0, borderRightWidth: 0, borderBottomWidth: StyleSheet.hairlineWidth },
  batterHead: { flexDirection: "row", alignItems: "flex-start", justifyContent: "space-between", gap: 8 },
  batterHeadRight: { flexDirection: "column", alignItems: "flex-end", gap: 6 },
  batterName: { color: "#E5E7EB", fontSize: 15, fontWeight: "700" },
  batterSub: { color: "#64748B", fontSize: 11, marginTop: 2 },
  gradePill: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 999,
    backgroundColor: "rgba(15,23,42,0.7)",
    paddingHorizontal: 10,
    paddingVertical: 4,
  },
  gradeText: { fontSize: 11, fontWeight: "800" },
  saveButton: {
    borderWidth: 1,
    borderRadius: 999,
    paddingHorizontal: 10,
    paddingVertical: 4,
  },
  saveButtonText: { fontSize: 11, fontWeight: "700" },

  // Odds row
  oddsRow: { flexDirection: "row", gap: 6, flexWrap: "wrap" },
  oddsPill: {
    borderWidth: StyleSheet.hairlineWidth,
    borderColor: "#334155",
    borderRadius: 6,
    paddingHorizontal: 8,
    paddingVertical: 4,
    backgroundColor: "#0F172A",
    alignItems: "center",
  },
  oddsPillLabel: { color: "#64748B", fontSize: 9, fontWeight: "700" },
  oddsPillValue: { color: "#93C5FD", fontSize: 12, fontWeight: "800" },

  // Metrics table
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
  emptySub: { color: "#A7C0E8", marginTop: 6, fontSize: 12, padding: 12 },
});

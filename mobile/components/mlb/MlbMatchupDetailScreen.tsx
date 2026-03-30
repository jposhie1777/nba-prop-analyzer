import { ActivityIndicator, Image, Linking, Pressable, ScrollView, StyleSheet, Text, View } from "react-native";
import { useLocalSearchParams, useRouter } from "expo-router";
import { useEffect, useMemo, useState } from "react";

import { useMlbMatchupDetail, useMlbBattingOrder } from "@/hooks/mlb/useMlbMatchups";
import { useTheme } from "@/store/useTheme";
import { getMlbTeamLogo } from "@/utils/mlbLogos";
import { BackToHomeButton } from "@/components/navigation/BackToHomeButton";
import { usePropBetslip } from "@/store/usePropBetslip";
import { useBetslipDrawer } from "@/store/useBetslipDrawer";
import { buildParlayLinks, getBuildPlatform, type ParlayBatterInput } from "@/utils/parlayBuilder";

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

function formatAmericanOdds(value?: number | null): string {
if (value == null || Number.isNaN(value) || value === 0) return "—";
return value > 0 ? `+${Math.round(value)}` : `${Math.round(value)}`;
}

function formatHitterHand(batSide?: string | null): string {
const side = (batSide ?? "").toUpperCase();
if (side === "L") return "LHB";
if (side === "S") return "SHB";
return "RHB";
}

function formatRate(value?: number | null, digits = 3): string {
if (value == null || Number.isNaN(value)) return "—";
return Number(value).toFixed(digits);
}

function formatPercent(value?: number | null, digits = 1): string {
if (value == null || Number.isNaN(value)) return "—";
return `${Number(value).toFixed(digits)}%`;
}

function normalizePitchName(value?: string | null): string {
return (value ?? "").trim().toLowerCase();
}

function pitcherToneByMetric(metric: "ba" | "woba" | "slg" | "iso" | "whiff_pct" | "k_pct", value?: number | null) {
if (value == null || Number.isNaN(value)) return styles.pitchStatNeutral;
if (metric === "ba") {
if (value < 0.23) return styles.pitchStatGood;
if (value > 0.28) return styles.pitchStatBad;
return styles.pitchStatNeutral;
}
if (metric === "woba") {
if (value < 0.29) return styles.pitchStatGood;
if (value > 0.35) return styles.pitchStatBad;
return styles.pitchStatNeutral;
}
if (metric === "slg") {
if (value < 0.38) return styles.pitchStatGood;
if (value > 0.48) return styles.pitchStatBad;
return styles.pitchStatNeutral;
}
if (metric === "iso") {
if (value < 0.14) return styles.pitchStatGood;
if (value > 0.2) return styles.pitchStatBad;
return styles.pitchStatNeutral;
}
if (metric === "whiff_pct" || metric === "k_pct") {
if (value > 25) return styles.pitchStatGood;
if (value < 15) return styles.pitchStatBad;
return styles.pitchStatNeutral;
}
return styles.pitchStatNeutral;
}

function hitterToneByMetric(metric: "ba" | "slg" | "iso" | "ev" | "barrel_pct", value?: number | null) {
if (value == null || Number.isNaN(value)) return styles.pitchStatNeutral;
if (metric === "ba") {
if (value > 0.28) return styles.pitchStatGood;
if (value < 0.23) return styles.pitchStatBad;
return styles.pitchStatNeutral;
}
if (metric === "slg") {
if (value > 0.48) return styles.pitchStatGood;
if (value < 0.38) return styles.pitchStatBad;
return styles.pitchStatNeutral;
}
if (metric === "iso") {
if (value > 0.2) return styles.pitchStatGood;
if (value < 0.14) return styles.pitchStatBad;
return styles.pitchStatNeutral;
}
if (metric === "ev") {
if (value > 92) return styles.pitchStatGood;
if (value < 87) return styles.pitchStatBad;
return styles.pitchStatNeutral;
}
if (metric === "barrel_pct") {
if (value > 12) return styles.pitchStatGood;
if (value < 6) return styles.pitchStatBad;
return styles.pitchStatNeutral;
}
return styles.pitchStatNeutral;
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

// ── Collapsible pitch analysis section ──────────────────────────────────────
function PitchAnalysisSection({
batter,
pitcher,
}: {
batter: any;
pitcher: any;
}) {
const [expanded, setExpanded] = useState(false);

// Auto-derive the correct hand keys from the data
// Pitcher mix: show pitches thrown to this batter's handedness
const batterHand = (batter.bat_side ?? "R").toUpperCase();
const pitcherMixKey = batterHand === "L" ? "vs_lhb" : "vs_rhb";
const pitcherMixLabel = batterHand === "L" ? "vs LHB" : "vs RHB";

// Hitter stats: show how this batter hits vs this pitcher's handedness
const pitcherHandRaw = (pitcher.pitcher_hand ?? "R").toUpperCase();
const pitcherHandChar = pitcherHandRaw.startsWith("L") ? "L" : "R";
const hitterStatsKey = pitcherHandChar === "L" ? "vs_lhp" : "vs_rhp";
const hitterStatsLabel = pitcherHandChar === "L" ? "vs LHP" : "vs RHP";

const pitcherRows: any[] = batter.pitcher_pitch_mix?.[pitcherMixKey] ?? [];

// Build a set of normalized pitch names from the pitcher mix
const pitcherPitchSet = useMemo(
() => new Set(pitcherRows.map((r: any) => normalizePitchName(r.pitch_name))),
[pitcherRows]
);

const allHitterRows: any[] = batter.hitter_stats_vs_pitches?.[hitterStatsKey] ?? [];

// Filter hitter rows to only pitches the pitcher actually throws
const hitterRows = useMemo(
() => allHitterRows.filter((r: any) => pitcherPitchSet.has(normalizePitchName(r.pitch_name))),
[allHitterRows, pitcherPitchSet]
);

const hasPitcherData = pitcherRows.length > 0;
const hasHitterData = hitterRows.length > 0;
const hasAnyData = hasPitcherData || hasHitterData;

return (
<View style={styles.pitchSection}>
<Pressable style={styles.pitchSectionToggle} onPress={() => setExpanded((v) => !v)}>
<View style={styles.pitchSectionToggleLeft}>
<Text style={styles.pitchSectionToggleIcon}>{expanded ? "▾" : "▸"}</Text>
<Text style={styles.pitchSectionToggleLabel}>Pitch Breakdown</Text>
<Text style={styles.pitchSectionToggleSub}>
{pitcherMixLabel} · {hitterStatsLabel}
</Text>
</View>
{!hasAnyData ? (
<Text style={styles.pitchSectionNoData}>No data</Text>
) : (
<Text style={styles.pitchSectionCount}>
{pitcherRows.length} pitch{pitcherRows.length !== 1 ? "es" : ""}
</Text>
)}
</Pressable>

  {expanded ? (
    <View style={styles.pitchSectionBody}>
      {/* ── Pitcher Pitch Mix ── */}
      <Text style={styles.pitchSubHead}>
        Pitcher Pitch Mix <Text style={styles.pitchSubHeadSub}>({pitcherMixLabel})</Text>
      </Text>
      {!hasPitcherData ? (
        <Text style={styles.pitchEmpty}>No pitch mix data available</Text>
      ) : (
        <ScrollView horizontal showsHorizontalScrollIndicator={false}>
          <View>
            <View style={styles.pitchTableHeader}>
              <Text style={[styles.pitchTableHeadCell, styles.pitchTypeCol]}>TYPE</Text>
              <Text style={styles.pitchTableHeadCell}>#</Text>
              <Text style={styles.pitchTableHeadCell}>%</Text>
              <Text style={styles.pitchTableHeadCell}>BA</Text>
              <Text style={styles.pitchTableHeadCell}>WOBA</Text>
              <Text style={styles.pitchTableHeadCell}>SLG</Text>
              <Text style={styles.pitchTableHeadCell}>ISO</Text>
              <Text style={styles.pitchTableHeadCell}>HR</Text>
              <Text style={styles.pitchTableHeadCell}>K%</Text>
              <Text style={styles.pitchTableHeadCell}>WHIFF%</Text>
            </View>
            {pitcherRows.map((row: any, idx: number) => (
              <View key={`pmix-${normalizePitchName(row.pitch_name)}-${idx}`} style={styles.pitchTableRow}>
                <Text style={[styles.pitchTableCell, styles.pitchTypeCol]}>{row.pitch_name ?? "—"}</Text>
                <Text style={styles.pitchTableCell}>{formatMetric(row.pitch_count, 0)}</Text>
                <Text style={styles.pitchTableCell}>{formatPercent(row.pitch_pct, 1)}</Text>
                <Text style={[styles.pitchTableCell, pitcherToneByMetric("ba", row.ba)]}>{formatRate(row.ba, 3)}</Text>
                <Text style={[styles.pitchTableCell, pitcherToneByMetric("woba", row.woba)]}>{formatRate(row.woba, 3)}</Text>
                <Text style={[styles.pitchTableCell, pitcherToneByMetric("slg", row.slg)]}>{formatRate(row.slg, 3)}</Text>
                <Text style={[styles.pitchTableCell, pitcherToneByMetric("iso", row.iso)]}>{formatRate(row.iso, 3)}</Text>
                <Text style={styles.pitchTableCell}>{formatMetric(row.hr, 0)}</Text>
                <Text style={[styles.pitchTableCell, pitcherToneByMetric("k_pct", row.k_pct)]}>{formatPercent(row.k_pct, 1)}</Text>
                <Text style={[styles.pitchTableCell, pitcherToneByMetric("whiff_pct", row.whiff_pct)]}>{formatPercent(row.whiff_pct, 1)}</Text>
              </View>
            ))}
          </View>
        </ScrollView>
      )}

      {/* ── Divider ── */}
      <View style={styles.pitchDivider} />

      {/* ── Hitter Stats Against Those Pitches ── */}
      <Text style={styles.pitchSubHead}>
        Hitter vs These Pitches <Text style={styles.pitchSubHeadSub}>({hitterStatsLabel})</Text>
      </Text>
      {!hasHitterData ? (
        <Text style={styles.pitchEmpty}>
          {allHitterRows.length > 0
            ? "No data for pitcher's specific pitches"
            : "No hitter pitch data available"}
        </Text>
      ) : (
        <ScrollView horizontal showsHorizontalScrollIndicator={false}>
          <View>
            <View style={styles.pitchTableHeader}>
              <Text style={[styles.pitchTableHeadCell, styles.pitchTypeCol]}>TYPE</Text>
              <Text style={styles.pitchTableHeadCell}>#</Text>
              <Text style={styles.pitchTableHeadCell}>%</Text>
              <Text style={styles.pitchTableHeadCell}>BA</Text>
              <Text style={styles.pitchTableHeadCell}>WOBA</Text>
              <Text style={styles.pitchTableHeadCell}>SLG</Text>
              <Text style={styles.pitchTableHeadCell}>ISO</Text>
              <Text style={styles.pitchTableHeadCell}>HR</Text>
              <Text style={styles.pitchTableHeadCell}>EV</Text>
              <Text style={styles.pitchTableHeadCell}>Barrel%</Text>
            </View>
            {hitterRows.map((row: any, idx: number) => {
              const totalCount = hitterRows.reduce((acc: number, r: any) => acc + (r.pitch_count ?? 0), 0);
              const pct = totalCount > 0 ? ((row.pitch_count ?? 0) / totalCount) * 100 : null;
              return (
                <View key={`hstat-${normalizePitchName(row.pitch_name)}-${idx}`} style={styles.pitchTableRow}>
                  <Text style={[styles.pitchTableCell, styles.pitchTypeCol]}>{row.pitch_name ?? "—"}</Text>
                  <Text style={styles.pitchTableCell}>{formatMetric(row.pitch_count, 0)}</Text>
                  <Text style={styles.pitchTableCell}>{formatPercent(pct, 1)}</Text>
                  <Text style={[styles.pitchTableCell, hitterToneByMetric("ba", row.ba)]}>{formatRate(row.ba, 3)}</Text>
                  <Text style={styles.pitchTableCell}>{formatRate(row.woba, 3)}</Text>
                  <Text style={[styles.pitchTableCell, hitterToneByMetric("slg", row.slg)]}>{formatRate(row.slg, 3)}</Text>
                  <Text style={[styles.pitchTableCell, hitterToneByMetric("iso", row.iso)]}>{formatRate(row.iso, 3)}</Text>
                  <Text style={styles.pitchTableCell}>{formatMetric(row.hr, 0)}</Text>
                  <Text style={[styles.pitchTableCell, hitterToneByMetric("ev", row.ev)]}>{formatMetric(row.ev, 1)}</Text>
                  <Text style={[styles.pitchTableCell, hitterToneByMetric("barrel_pct", row.barrel_pct)]}>{formatPercent(row.barrel_pct, 1)}</Text>
                </View>
              );
            })}
          </View>
        </ScrollView>
      )}
    </View>
  ) : null}
</View>

);
}

export function MlbMatchupDetailScreen() {
const { colors } = useTheme();
const router = useRouter();
const params = useLocalSearchParams<{ gamePk?: string; awayTeam?: string; homeTeam?: string }>();
const gamePk = Number(params.gamePk);
const { data, loading, error, refetch } = useMlbMatchupDetail(Number.isFinite(gamePk) ? gamePk : null);
const { data: boData } = useMlbBattingOrder(Number.isFinite(gamePk) ? gamePk : null);

// Build weak spot lookup: pitcher_id -> Set of player_ids in weak spots
const weakSpotMap = useMemo(() => {
  const map = new Map<number, Set<number>>();
  for (const pitcher of boData?.pitchers ?? []) {
    const pid = pitcher.pitcher_id;
    if (pid == null) continue;
    const weakIds = new Set<number>();
    for (const pos of pitcher.positions ?? []) {
      if (pos.is_weak_spot && pos.player_id != null) {
        weakIds.add(pos.player_id);
      }
    }
    map.set(pid, weakIds);
  }
  return map;
}, [boData]);

const game = data?.game;
const awayTeam = params.awayTeam ?? game?.away_team ?? "Away";
const homeTeam = params.homeTeam ?? game?.home_team ?? "Home";
const topCount = (data?.grade_counts?.IDEAL ?? 0) + (data?.grade_counts?.FAVORABLE ?? 0);
const awayLogo = getMlbTeamLogo(awayTeam);
const homeLogo = getMlbTeamLogo(homeTeam);
const [selectedKeys, setSelectedKeys] = useState<Set<string>>(new Set());
const betslipItems = usePropBetslip((s) => s.items);
const addToBetslip = usePropBetslip((s) => s.add);
const removeFromBetslip = usePropBetslip((s) => s.remove);
const openBetslip = useBetslipDrawer((s) => s.open);
const platform = getBuildPlatform();

const allVisibleBatters = useMemo(() => {
const out: {
key: string;
batter: any;
pitcher: any;
teamName: string;
}[] = [];
for (const pitcher of data?.pitchers ?? []) {
const batters = dedupeBatters(pitcher.batters ?? []).slice(0, 12);
for (const batter of batters) {
const key = `${String(pitcher.pitcher_id)}-${String(batter.batter_id ?? batter.batter_name ?? "")}`;
out.push({ key, batter, pitcher, teamName: pitcher.offense_team ?? "Team" });
}
}
return out;
}, [data?.pitchers]);

const selectedBatters = useMemo(
() => allVisibleBatters.filter((entry) => selectedKeys.has(entry.key)),
[allVisibleBatters, selectedKeys]
);

const selectedParlayInput: ParlayBatterInput[] = useMemo(
() =>
selectedBatters.map(({ batter }) => ({
batter_id: batter.batter_id ?? null,
batter_name: batter.batter_name ?? null,
score: batter.score ?? null,
hr_odds_best_price: batter.hr_odds_best_price ?? null,
dk_outcome_code: batter.dk_outcome_code ?? null,
dk_event_id: batter.dk_event_id ?? null,
fd_market_id: batter.fd_market_id ?? null,
fd_selection_id: batter.fd_selection_id ?? null,
})),
[selectedBatters]
);

const parlayLinks = useMemo(
() => buildParlayLinks(selectedParlayInput, platform),
[selectedParlayInput, platform]
);

function makeMlbSlipId(batter: any): string {
return `mlb-hr-${String(data?.game_pk ?? 0)}-${String(batter.batter_id ?? batter.batter_name ?? "")}`;
}

function addBatterToMlbSlip(batter: any, pitcher: any) {
addToBetslip({
id: makeMlbSlipId(batter),
player_id: Number(batter.batter_id ?? 0),
player: batter.batter_name ?? "Batter",
market: "MLB 1+ HR",
side: "over",
line: 0.5,
odds: Number(batter.hr_odds_best_price ?? 100),
matchup: `${pitcher.offense_team ?? ""} vs ${pitcher.pitcher_name ?? ""}`,
sport: "mlb",
bookmaker: null,
dk_event_id: batter.dk_event_id ?? null,
dk_outcome_code: batter.dk_outcome_code ?? null,
fd_market_id: batter.fd_market_id ?? null,
fd_selection_id: batter.fd_selection_id ?? null,
});
}

function toggleSelected(key: string, batter: any, pitcher: any) {
const slipId = makeMlbSlipId(batter);
setSelectedKeys((prev) => {
const next = new Set(prev);
if (next.has(key)) {
next.delete(key);
removeFromBetslip(slipId);
} else if (next.size < 10) {
next.add(key);
addBatterToMlbSlip(batter, pitcher);
openBetslip();
}
return next;
});
}

function clearSelections() {
selectedBatters.forEach(({ batter }) => removeFromBetslip(makeMlbSlipId(batter)));
setSelectedKeys(new Set());
}

useEffect(() => {
const next = new Set<string>();
for (const entry of allVisibleBatters) {
if (betslipItems.some((item) => item.id === makeMlbSlipId(entry.batter))) {
next.add(entry.key);
}
}
setSelectedKeys(next);
// eslint-disable-next-line react-hooks/exhaustive-deps
}, [betslipItems, allVisibleBatters]);

function openUrl(url?: string | null) {
if (!url) return;
if (platform === "desktop" && typeof globalThis.open === "function") {
globalThis.open(url, "_blank");
return;
}
Linking.openURL(url).catch(() => {});
}

function singleLegLink(batter: any, book: "draftkings" | "fanduel"): string | null {
const single = buildParlayLinks(
[
{
batter_id: batter.batter_id ?? null,
batter_name: batter.batter_name ?? null,
score: batter.score ?? null,
hr_odds_best_price: batter.hr_odds_best_price ?? null,
dk_outcome_code: batter.dk_outcome_code ?? null,
dk_event_id: batter.dk_event_id ?? null,
fd_market_id: batter.fd_market_id ?? null,
fd_selection_id: batter.fd_selection_id ?? null,
},
],
platform
);
return book === "draftkings" ? single.draftkings : single.fanduel;
}

return (
<ScrollView style={styles.screen} contentContainerStyle={styles.content}>
{/* ── Navigation ── */}
<View style={styles.actionRow}>
<Pressable onPress={() => router.push("/(tabs)/mlb" as any)} style={styles.navBtn}>
  <Text style={styles.navBtnText}>← MLB</Text>
</Pressable>
<Pressable onPress={() => router.push("/(tabs)/home")} style={styles.navBtn}>
  <Text style={styles.navBtnText}>Home</Text>
</Pressable>
</View>

{/* ── Sub-tab indicator ── */}
<View style={styles.tabRow}>
<View style={styles.tabActive}>
  <Text style={styles.tabTextActive}>Home Runs</Text>
</View>
<Pressable
  onPress={() =>
    router.push({
      pathname: "/(tabs)/mlb/pitching-props/[gamePk]" as any,
      params: { gamePk: String(gamePk), homeTeam, awayTeam },
    })
  }
  style={styles.tabInactive}
>
  <Text style={styles.tabTextInactive}>Pitching Props</Text>
</Pressable>
<Pressable
  onPress={() =>
    router.push({
      pathname: "/(tabs)/mlb/lineup-matchup/[gamePk]" as any,
      params: { gamePk: String(gamePk), homeTeam, awayTeam },
    })
  }
  style={styles.tabInactive}
>
  <Text style={styles.tabTextInactive}>Lineup</Text>
</Pressable>
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
      <View style={styles.pill}>
        <Text style={styles.pillText}>
          ☁ {game?.weather?.weather_indicator ?? "Weather"}{" "}
          {game?.weather?.game_temp != null ? `${Math.round(game.weather.game_temp)}°` : ""}
          {game?.weather?.wind_speed != null
            ? ` • ${Math.round(game.weather.wind_speed)} mph ${game?.weather?.wind_direction_label ?? ""}`
            : ""}
        </Text>
      </View>
      <View style={styles.pill}>
        <Text style={styles.pillText}>🏟 {game?.weather?.ballpark_name ?? game?.venue_name ?? "Park TBD"}</Text>
      </View>
      <View style={styles.pill}>
        <Text style={styles.pillText}>
          💰 ML {formatAmericanOdds(game?.odds?.away_moneyline)} / {formatAmericanOdds(game?.odds?.home_moneyline)}{" "}
          {game?.odds?.over_under != null ? `• O/U ${game.odds.over_under}` : ""}
        </Text>
      </View>
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
          const batterKey = `${String(pitcher.pitcher_id)}-${String(batter.batter_id ?? batter.batter_name ?? "")}`;
          const isSelected = selectedKeys.has(batterKey);
          const isWeakSpot = batter.batter_id != null
            && pitcher.pitcher_id != null
            && (weakSpotMap.get(pitcher.pitcher_id)?.has(batter.batter_id) ?? false);
          return (
            <View
              key={`${pitcher.pitcher_id}-${batter.batter_id ?? batter.batter_name}`}
              style={[
                styles.batterCard,
                { borderColor: isWeakSpot ? "#10B981" : tone.border, backgroundColor: isWeakSpot ? "rgba(16,185,129,0.08)" : tone.bg },
                isSelected ? styles.batterCardSelected : null,
              ]}
            >
              <View style={styles.batterHead}>
                <Text style={styles.batterName}>
                  {isWeakSpot ? "🎯 " : ""}{batter.batter_name ?? "Batter"} - {formatHitterHand(batter.bat_side)}
                </Text>
                {isWeakSpot ? (
                  <View style={styles.weakSpotPill}>
                    <Text style={styles.weakSpotText}>WEAK SPOT</Text>
                  </View>
                ) : null}
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

              {/* ── Collapsible pitch breakdown ── */}
              <PitchAnalysisSection batter={batter} pitcher={pitcher} />

              <View style={styles.betRow}>
                <Pressable style={styles.selectBtn} onPress={() => toggleSelected(batterKey, batter, pitcher)}>
                  <Text style={styles.selectBtnText}>{isSelected ? "☑ Selected" : "☐ Select for Parlay"}</Text>
                </Pressable>
                <Text style={styles.betPriceText}>1+ HR {formatAmericanOdds(batter.hr_odds_best_price)}</Text>
              </View>
              <View style={styles.betRow}>
                <Pressable
                  style={[styles.bookBtn, !singleLegLink(batter, "draftkings") ? styles.bookBtnDisabled : null]}
                  disabled={!singleLegLink(batter, "draftkings")}
                  onPress={() => {
                    const url = singleLegLink(batter, "draftkings");
                    if (!url) return;
                    addBatterToMlbSlip(batter, pitcher);
                    openBetslip();
                    openUrl(url);
                  }}
                >
                  <Text style={styles.bookBtnText}>Bet DraftKings</Text>
                </Pressable>
                <Pressable
                  style={[styles.bookBtn, !singleLegLink(batter, "fanduel") ? styles.bookBtnDisabled : null]}
                  disabled={!singleLegLink(batter, "fanduel")}
                  onPress={() => {
                    const url = singleLegLink(batter, "fanduel");
                    if (!url) return;
                    addBatterToMlbSlip(batter, pitcher);
                    openBetslip();
                    openUrl(url);
                  }}
                >
                  <Text style={styles.bookBtnText}>Bet FanDuel</Text>
                </Pressable>
              </View>
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

  {selectedBatters.length >= 2 ? (
    <View style={styles.parlayBar}>
      <View style={styles.parlayTopRow}>
        <Text style={styles.parlayTitle}>{selectedBatters.length} batters selected</Text>
        <Pressable onPress={clearSelections}>
          <Text style={styles.parlayClear}>✕</Text>
        </Pressable>
      </View>
      <Text style={styles.parlayOddsText}>
        Combined Odds: {formatAmericanOdds(parlayLinks.combinedOdds)}
      </Text>
      <View style={styles.parlayBtnRow}>
        <Pressable
          style={[styles.parlayBtn, !parlayLinks.draftkings ? styles.bookBtnDisabled : null]}
          disabled={!parlayLinks.draftkings}
          onPress={() => {
            openBetslip();
            openUrl(parlayLinks.draftkings);
          }}
        >
          <Text style={styles.parlayBtnText}>Bet DraftKings</Text>
        </Pressable>
        <Pressable
          style={[styles.parlayBtn, !parlayLinks.fanduel ? styles.bookBtnDisabled : null]}
          disabled={!parlayLinks.fanduel}
          onPress={() => {
            openBetslip();
            openUrl(parlayLinks.fanduel);
          }}
        >
          <Text style={styles.parlayBtnText}>Bet FanDuel</Text>
        </Pressable>
      </View>
      <Text style={styles.parlayNote}>Parlay availability subject to sportsbook approval.</Text>
      <View style={styles.parlaySummary}>
        {selectedBatters.map(({ key, batter, teamName }) => (
          <Text key={key} style={styles.parlayLegText}>
            • {batter.batter_name ?? "Batter"} ({teamName}) • Pulse {formatScore(batter.score)} •{" "}
            {formatAmericanOdds(batter.hr_odds_best_price)}
          </Text>
        ))}
      </View>
    </View>
  ) : null}
</ScrollView>

);
}

const styles = StyleSheet.create({
screen: { flex: 1, backgroundColor: "#050A18" },
content: { padding: 16, gap: 10, paddingBottom: 40 },
actionRow: { flexDirection: "row", gap: 8, marginBottom: 2 },
navBtn: { borderWidth: StyleSheet.hairlineWidth, borderColor: "#334155", borderRadius: 8, paddingHorizontal: 12, paddingVertical: 6, backgroundColor: "#0F172A" },
navBtnText: { color: "#93C5FD", fontSize: 12, fontWeight: "700" },
tabRow: { flexDirection: "row", gap: 0, marginBottom: 4 },
tabActive: { flex: 1, borderBottomWidth: 2, borderBottomColor: "#10B981", paddingVertical: 10, alignItems: "center" },
tabInactive: { flex: 1, borderBottomWidth: 2, borderBottomColor: "#1E293B", paddingVertical: 10, alignItems: "center" },
tabTextActive: { color: "#10B981", fontSize: 13, fontWeight: "800" },
tabTextInactive: { color: "#64748B", fontSize: 13, fontWeight: "700" },
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
batterCardSelected: { shadowColor: "#22D3EE", shadowOpacity: 0.35, shadowRadius: 8, borderWidth: 1.25 },
weakSpotPill: { borderWidth: 1, borderColor: "#10B981", borderRadius: 999, paddingHorizontal: 6, paddingVertical: 2, backgroundColor: "rgba(16,185,129,0.15)" },
weakSpotText: { color: "#10B981", fontSize: 8, fontWeight: "800" },
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

// ── Pitch analysis collapsible section ──
pitchSection: {
borderWidth: StyleSheet.hairlineWidth,
borderColor: "#334155",
borderRadius: 8,
backgroundColor: "rgba(15,23,42,0.28)",
overflow: "hidden",
},
pitchSectionToggle: {
flexDirection: "row",
alignItems: "center",
justifyContent: "space-between",
paddingHorizontal: 10,
paddingVertical: 9,
},
pitchSectionToggleLeft: { flexDirection: "row", alignItems: "center", gap: 6, flex: 1 },
pitchSectionToggleIcon: { color: "#64748B", fontSize: 12, width: 12 },
pitchSectionToggleLabel: { color: "#CBD5E1", fontSize: 12, fontWeight: "700" },
pitchSectionToggleSub: { color: "#64748B", fontSize: 11 },
pitchSectionNoData: { color: "#475569", fontSize: 10 },
pitchSectionCount: { color: "#475569", fontSize: 10 },
pitchSectionBody: {
borderTopWidth: StyleSheet.hairlineWidth,
borderTopColor: "#1E293B",
paddingHorizontal: 8,
paddingVertical: 10,
gap: 8,
},
pitchSubHead: { color: "#94A3B8", fontSize: 11, fontWeight: "700" },
pitchSubHeadSub: { color: "#475569", fontSize: 10, fontWeight: "400" },
pitchDivider: {
height: StyleSheet.hairlineWidth,
backgroundColor: "#1E293B",
marginVertical: 4,
},

pitchEmpty: { color: "#94A3B8", fontSize: 11, paddingVertical: 8 },
pitchTableHeader: {
flexDirection: "row",
backgroundColor: "#0F172A",
borderBottomWidth: StyleSheet.hairlineWidth,
borderBottomColor: "#334155",
},
pitchTableRow: {
flexDirection: "row",
borderBottomWidth: StyleSheet.hairlineWidth,
borderBottomColor: "rgba(51,65,85,0.65)",
backgroundColor: "rgba(15,23,42,0.22)",
},
pitchTableHeadCell: {
width: 56,
color: "#94A3B8",
fontSize: 9,
fontWeight: "800",
textAlign: "center",
paddingVertical: 6,
},
pitchTableCell: {
width: 56,
color: "#E2E8F0",
fontSize: 10,
fontWeight: "700",
textAlign: "center",
paddingVertical: 6,
},
pitchTypeCol: { width: 108, textAlign: "left", paddingLeft: 6 },
pitchStatGood: { color: "#34D399" },
pitchStatBad: { color: "#F87171" },
pitchStatNeutral: { color: "#E2E8F0" },

betRow: { flexDirection: "row", gap: 8, alignItems: "center" },
selectBtn: {
flex: 1,
borderWidth: StyleSheet.hairlineWidth,
borderColor: "#334155",
borderRadius: 10,
paddingVertical: 8,
paddingHorizontal: 10,
backgroundColor: "#0F172A",
},
selectBtnText: { color: "#C7D2FE", fontSize: 12, fontWeight: "700", textAlign: "center" },
betPriceText: { color: "#86EFAC", fontSize: 12, fontWeight: "800", minWidth: 90, textAlign: "right" },
bookBtn: {
flex: 1,
borderWidth: StyleSheet.hairlineWidth,
borderColor: "#334155",
borderRadius: 10,
backgroundColor: "#111827",
paddingVertical: 9,
alignItems: "center",
},
bookBtnDisabled: { opacity: 0.45 },
bookBtnText: { color: "#E5E7EB", fontSize: 12, fontWeight: "800" },
parlayBar: {
borderWidth: StyleSheet.hairlineWidth,
borderColor: "#334155",
borderRadius: 14,
backgroundColor: "rgba(2,6,23,0.98)",
padding: 12,
gap: 8,
marginTop: 8,
},
parlayTopRow: { flexDirection: "row", justifyContent: "space-between", alignItems: "center" },
parlayTitle: { color: "#E2E8F0", fontSize: 14, fontWeight: "800" },
parlayClear: { color: "#94A3B8", fontSize: 16, fontWeight: "900" },
parlayOddsText: { color: "#86EFAC", fontSize: 13, fontWeight: "800" },
parlayBtnRow: { flexDirection: "row", gap: 8 },
parlayBtn: {
flex: 1,
borderWidth: StyleSheet.hairlineWidth,
borderColor: "#334155",
borderRadius: 10,
backgroundColor: "#0F172A",
paddingVertical: 10,
alignItems: "center",
},
parlayBtnText: { color: "#E5E7EB", fontSize: 12, fontWeight: "800" },
parlayNote: { color: "#94A3B8", fontSize: 10 },
parlaySummary: { gap: 4 },
parlayLegText: { color: "#C7D2FE", fontSize: 11 },
errorBox: { borderWidth: StyleSheet.hairlineWidth, borderRadius: 12, backgroundColor: "#1F2937", padding: 12 },
errorTitle: { color: "#FCA5A5", fontWeight: "700" },
errorText: { color: "#FECACA", marginTop: 4, fontSize: 12 },
errorRetry: { color: "#E5E7EB", marginTop: 8, fontSize: 12 },
emptyTitle: { color: "#E5E7EB", fontWeight: "700" },
emptySub: { color: "#A7C0E8", marginTop: 6, fontSize: 12 },
});
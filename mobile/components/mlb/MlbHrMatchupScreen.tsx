import {
  ActivityIndicator,
  Image,
  Pressable,
  ScrollView,
  StyleSheet,
  Text,
  View,
} from "react-native";
import { useLocalSearchParams, useRouter } from "expo-router";
import { useMemo, useState } from "react";

import {
  useMlbMatchupDetail,
  type MlbBatterPick,
  type MlbPitcherGroup,
  type MlbPitchMixRow,
  type MlbBatterVsPitchRow,
} from "@/hooks/mlb/useMlbMatchups";
import { useTheme } from "@/store/useTheme";
import { getMlbTeamLogo } from "@/utils/mlbLogos";

// ── Formatting helpers ──────────────────────────────────────────────────────

function fmt(value?: number | null, digits = 3): string {
  if (value == null || !Number.isFinite(value)) return "—";
  return value.toFixed(digits);
}

function fmtPct(value?: number | null, digits = 1): string {
  if (value == null || !Number.isFinite(value)) return "—";
  return `${value.toFixed(digits)}%`;
}

function fmtOdds(value?: number | null): string {
  if (value == null || !Number.isFinite(value) || value === 0) return "—";
  return value > 0 ? `+${Math.round(value)}` : `${Math.round(value)}`;
}

function handLabel(side?: string | null): string {
  const s = (side ?? "").toUpperCase();
  if (s === "L") return "LHB";
  if (s === "S") return "SHB";
  return "RHB";
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
  if (g === "IDEAL") return { border: "#10B981", bg: "rgba(16,185,129,0.12)", text: "#A7F3D0", short: "A+" };
  if (g === "FAVORABLE") return { border: "#22D3EE", bg: "rgba(34,211,238,0.12)", text: "#CFFAFE", short: "A" };
  if (g === "AVOID") return { border: "#EF4444", bg: "rgba(239,68,68,0.12)", text: "#FECACA", short: "D" };
  return { border: "#F59E0B", bg: "rgba(245,158,11,0.12)", text: "#FDE68A", short: "B" };
}

// Green shading for good batter values (no red per user request)
function greenTone(metric: string, value?: number | null) {
  if (value == null || !Number.isFinite(value)) return null;
  switch (metric) {
    case "avg":
      if (value > 0.28) return s.cellGreen;
      break;
    case "iso":
      if (value > 0.2) return s.cellGreen;
      break;
    case "barrel_pct":
      if (value > 12) return s.cellGreen;
      break;
    case "hh_pct":
      if (value > 38) return s.cellGreen;
      break;
    case "slg":
      if (value > 0.48) return s.cellGreen;
      break;
    case "woba":
      if (value > 0.35) return s.cellGreen;
      break;
    case "obp":
      if (value > 0.35) return s.cellGreen;
      break;
  }
  return null;
}

function normPitch(name?: string | null): string {
  return (name ?? "").trim().toLowerCase();
}

// ── Aggregate batter stats for selected pitches ─────────────────────────────

type AggregatedStats = {
  avg: number | null;
  iso: number | null;
  barrel_pct_l15: number | null;
  hh_pct: number | null;
  slg: number | null;
  woba: number | null;
  obp: number | null;
};

function aggregateStatsForPitches(
  pitchRows: MlbBatterVsPitchRow[],
  selectedPitches: Set<string>,
  batter: MlbBatterPick
): AggregatedStats {
  const filtered = pitchRows.filter((r) => selectedPitches.has(normPitch(r.pitch_name)));
  if (filtered.length === 0) {
    return {
      avg: null,
      iso: batter.iso ?? null,
      barrel_pct_l15: batter.l15_barrel_pct ?? null,
      hh_pct: batter.l15_hard_hit_pct ?? null,
      slg: batter.slg ?? null,
      woba: null,
      obp: null,
    };
  }

  // Weighted average by pitch_count
  let totalCount = 0;
  let sumBa = 0;
  let sumIso = 0;
  let sumSlg = 0;
  let sumWoba = 0;
  let hasBa = false;
  let hasIso = false;
  let hasSlg = false;
  let hasWoba = false;

  for (const row of filtered) {
    const count = (row as any).pitch_count ?? row.count ?? 1;
    totalCount += count;
    if (row.ba != null) { sumBa += row.ba * count; hasBa = true; }
    if (row.iso != null) { sumIso += row.iso * count; hasIso = true; }
    if (row.slg != null) { sumSlg += row.slg * count; hasSlg = true; }
    if (row.woba != null) { sumWoba += row.woba * count; hasWoba = true; }
  }

  const w = totalCount || 1;
  return {
    avg: hasBa ? sumBa / w : null,
    iso: hasIso ? sumIso / w : (batter.iso ?? null),
    barrel_pct_l15: batter.l15_barrel_pct ?? null,
    hh_pct: batter.l15_hard_hit_pct ?? null,
    slg: hasSlg ? sumSlg / w : (batter.slg ?? null),
    woba: hasWoba ? sumWoba / w : null,
    obp: null, // Not available per-pitch; future enhancement
  };
}

// ── Dedupe batters ──────────────────────────────────────────────────────────

function dedupeBatters(batters: MlbBatterPick[]): MlbBatterPick[] {
  const byKey = new Map<string, MlbBatterPick>();
  for (const batter of batters) {
    const key = batter.batter_id != null ? `id:${batter.batter_id}` : `name:${(batter.batter_name ?? "").toLowerCase()}`;
    const existing = byKey.get(key);
    if (!existing || (batter.score ?? 0) > (existing.score ?? 0)) {
      byKey.set(key, batter);
    }
  }
  return Array.from(byKey.values()).sort((a, b) => (b.score ?? 0) - (a.score ?? 0));
}

// ── Pitch toggle button ─────────────────────────────────────────────────────

function PitchToggle({
  pitch,
  active,
  onToggle,
}: {
  pitch: MlbPitchMixRow;
  active: boolean;
  onToggle: () => void;
}) {
  return (
    <Pressable
      style={[s.pitchToggle, active ? s.pitchToggleActive : s.pitchToggleInactive]}
      onPress={onToggle}
    >
      <Text style={[s.pitchToggleText, active ? s.pitchToggleTextActive : null]} numberOfLines={1}>
        {pitch.pitch_name ?? "?"}{" "}
        <Text style={s.pitchTogglePct}>{pitch.pitch_pct != null ? `${pitch.pitch_pct.toFixed(0)}%` : ""}</Text>
      </Text>
    </Pressable>
  );
}

// ── Mini progress bar ───────────────────────────────────────────────────────

function MiniBar({ label, value, max = 100, color = "#10B981" }: { label: string; value?: number | null; max?: number; color?: string }) {
  const pct = value != null && Number.isFinite(value) ? Math.min(value / max, 1) : 0;
  return (
    <View style={s.miniBarWrap}>
      <Text style={s.miniBarLabel}>{label}</Text>
      <View style={s.miniBarTrack}>
        <View style={[s.miniBarFill, { width: `${Math.round(pct * 100)}%`, backgroundColor: color }]} />
      </View>
      <Text style={s.miniBarValue}>{value != null && Number.isFinite(value) ? `${value.toFixed(1)}%` : "—"}</Text>
    </View>
  );
}

// ── Batted ball profile + hit log ───────────────────────────────────────────

function BattedBallProfile({ batter }: { batter: MlbBatterPick }) {
  const bb = batter.bvp_batted_ball;
  if (!bb) return null;

  const profile = bb.profile;
  const log = bb.log ?? [];
  const hasProfile = profile != null && (profile.total_batted ?? 0) > 0;

  if (!hasProfile && log.length === 0) return null;

  return (
    <View style={s.bbWrap}>
      <Text style={s.expandedTitle}>
        Batted Ball History{profile?.total_batted ? ` (${profile.total_batted} batted)` : ""}
      </Text>

      {/* Mini bars */}
      {hasProfile ? (
        <View style={s.bbBarsContainer}>
          <View style={s.bbBarsRow}>
            <MiniBar label="Barrel%" value={profile!.barrel_pct} max={25} color="#10B981" />
            <MiniBar label="HH%" value={profile!.hh_pct} max={60} color="#10B981" />
            <MiniBar label="FB%" value={profile!.fb_pct} max={60} color="#60A5FA" />
            <MiniBar label="GB%" value={profile!.gb_pct} max={60} color="#F59E0B" />
            <MiniBar label="LD%" value={profile!.ld_pct} max={40} color="#34D399" />
          </View>
          <View style={s.bbBarsRow}>
            <MiniBar label="PU%" value={profile!.pu_pct} max={30} color="#94A3B8" />
            <MiniBar label="HR/FB%" value={profile!.hr_fb_pct} max={30} color="#F87171" />
            <MiniBar label="Pull%" value={profile!.pull_pct} max={60} color="#A78BFA" />
            <MiniBar label="Str%" value={profile!.str_pct} max={50} color="#A78BFA" />
            <MiniBar label="Oppo%" value={profile!.oppo_pct} max={40} color="#A78BFA" />
          </View>
        </View>
      ) : null}

      {/* Hit log table */}
      {log.length > 0 ? (
        <ScrollView horizontal showsHorizontalScrollIndicator={false}>
          <View>
            <View style={s.logHeaderRow}>
              <Text style={[s.logHeaderCell, s.logDateCol]}>DATE</Text>
              <Text style={[s.logHeaderCell, s.logPitchCol]}>PITCH</Text>
              <Text style={s.logHeaderCell}>EV</Text>
              <Text style={s.logHeaderCell}>ANGLE</Text>
              <Text style={s.logHeaderCell}>DIST</Text>
              <Text style={[s.logHeaderCell, s.logTrajCol]}>TRAJ</Text>
              <Text style={[s.logHeaderCell, s.logResultCol]}>RESULT</Text>
              <Text style={s.logHeaderCell}>PARKS</Text>
            </View>
            {log.map((row, idx) => {
              const ev = row.ev;
              const evStyle = ev != null && ev >= 95 ? s.cellGreen : null;
              const angleVal = row.angle;
              const angleLaunch = angleVal != null && angleVal >= 10 && angleVal <= 30;
              const isHr = (row.result ?? "").toLowerCase() === "home_run";
              return (
                <View key={`log-${idx}`} style={s.logRow}>
                  <Text style={[s.logCell, s.logDateCol]}>{row.date ?? "—"}</Text>
                  <Text style={[s.logCell, s.logPitchCol]}>{row.pitch ?? "—"}</Text>
                  <Text style={[s.logCell, evStyle]}>{ev != null ? ev.toFixed(1) : "—"}</Text>
                  <Text style={[s.logCell, angleLaunch ? s.cellGreen : null]}>{angleVal != null ? angleVal.toFixed(1) : "—"}</Text>
                  <Text style={s.logCell}>{row.dist != null ? row.dist.toFixed(0) : "—"}</Text>
                  <Text style={[s.logCell, s.logTrajCol]}>{formatTrajectory(row.trajectory)}</Text>
                  <Text style={[s.logCell, s.logResultCol, isHr ? s.cellGreen : null]}>{formatResult(row.result)}</Text>
                  <Text style={[s.logCell, (row.hr_parks ?? 0) >= 20 ? s.cellGreen : null]}>{row.hr_parks != null ? `${row.hr_parks}/30` : "—"}</Text>
                </View>
              );
            })}
          </View>
        </ScrollView>
      ) : null}
    </View>
  );
}

function formatTrajectory(traj?: string | null): string {
  if (!traj) return "—";
  return traj.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
}

function formatResult(result?: string | null): string {
  if (!result) return "—";
  return result.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
}

// ── Single batter row ───────────────────────────────────────────────────────

function BatterRow({
  batter,
  stats,
  expanded,
  onToggleExpand,
}: {
  batter: MlbBatterPick;
  stats: AggregatedStats;
  expanded: boolean;
  onToggleExpand: () => void;
}) {
  const tone = gradeTone(batter.grade);
  const bvp = batter.bvp_career;
  const hasBvp = bvp != null && (bvp.pa ?? 0) > 0;

  return (
    <View style={s.batterRowWrap}>
      <View style={s.batterRow}>
        {/* Chevron + player info (fixed left) */}
        <Pressable style={s.batterInfoCol} onPress={onToggleExpand}>
          <Text style={s.chevron}>{expanded ? "▾" : "▸"}</Text>
          <View style={[s.gradeChip, { backgroundColor: tone.bg, borderColor: tone.border }]}>
            <Text style={[s.gradeChipText, { color: tone.text }]}>{tone.short}</Text>
          </View>
          <View style={s.nameWrap}>
            <Text style={s.batterName} numberOfLines={1}>
              {batter.batter_name ?? "—"}
            </Text>
            <Text style={s.batterMeta}>{handLabel(batter.bat_side)}</Text>
          </View>
        </Pressable>

        {/* Scrollable stat cells */}
        <ScrollView horizontal showsHorizontalScrollIndicator={false} style={s.statsScroll}>
          <View style={s.statsRow}>
            <Text style={[s.statCell, greenTone("avg", stats.avg)]}>{fmt(stats.avg)}</Text>
            <Text style={[s.statCell, greenTone("iso", stats.iso)]}>{fmt(stats.iso)}</Text>
            <Text style={[s.statCell, greenTone("barrel_pct", stats.barrel_pct_l15)]}>{fmtPct(stats.barrel_pct_l15)}</Text>
            <Text style={[s.statCell, greenTone("hh_pct", stats.hh_pct)]}>{fmtPct(stats.hh_pct)}</Text>
            <Text style={[s.statCell, greenTone("slg", stats.slg)]}>{fmt(stats.slg)}</Text>
          </View>
        </ScrollView>
      </View>

      {/* Expanded: Career BvP + Batted Ball Profile + Hit Log */}
      {expanded ? (
        <View style={s.expandedArea}>
          {/* ── Career BvP stats ── */}
          <Text style={s.expandedTitle}>Career vs Pitcher</Text>
          {hasBvp ? (
            <View style={s.bvpWrap}>
              <View style={s.bvpSummary}>
                <Text style={s.bvpSummaryText}>
                  {bvp!.hits ?? 0}-{bvp!.pa ?? 0} · {bvp!.hr ?? 0} HR
                </Text>
              </View>
              <View style={s.bvpTable}>
                <View style={s.bvpHeaderRow}>
                  <Text style={s.bvpHeaderCell}>AVG</Text>
                  <Text style={s.bvpHeaderCell}>ISO</Text>
                  <Text style={s.bvpHeaderCell}>SLG</Text>
                  <Text style={s.bvpHeaderCell}>OBP</Text>
                  <Text style={s.bvpHeaderCell}>K%</Text>
                  <Text style={s.bvpHeaderCell}>BB%</Text>
                </View>
                <View style={s.bvpValueRow}>
                  <Text style={[s.bvpValueCell, greenTone("avg", bvp!.avg)]}>{fmt(bvp!.avg)}</Text>
                  <Text style={[s.bvpValueCell, greenTone("iso", bvp!.iso)]}>{fmt(bvp!.iso)}</Text>
                  <Text style={[s.bvpValueCell, greenTone("slg", bvp!.slg)]}>{fmt(bvp!.slg)}</Text>
                  <Text style={[s.bvpValueCell, greenTone("obp", bvp!.obp)]}>{fmt(bvp!.obp)}</Text>
                  <Text style={s.bvpValueCell}>{fmtPct(bvp!.k_pct)}</Text>
                  <Text style={s.bvpValueCell}>{fmtPct(bvp!.bb_pct)}</Text>
                </View>
              </View>
            </View>
          ) : (
            <Text style={s.bvpEmpty}>No career data vs this pitcher</Text>
          )}

          {/* ── Batted Ball Profile ── */}
          <BattedBallProfile batter={batter} />
        </View>
      ) : null}
    </View>
  );
}

// ── Handedness section (vs RHB or vs LHB) ───────────────────────────────────

function HandednessSection({
  label,
  batters,
  pitcherMix,
  pitcherHand,
}: {
  label: string;
  batters: MlbBatterPick[];
  pitcherMix: MlbPitchMixRow[];
  pitcherHand: string;
}) {
  // All pitches selected by default
  const [selectedPitches, setSelectedPitches] = useState<Set<string>>(
    () => new Set(pitcherMix.map((r) => normPitch(r.pitch_name)))
  );
  const [expandedIds, setExpandedIds] = useState<Set<string>>(new Set());

  const togglePitch = (pitchName: string) => {
    setSelectedPitches((prev) => {
      const next = new Set(prev);
      const key = normPitch(pitchName);
      if (next.has(key)) {
        if (next.size > 1) next.delete(key); // Keep at least one selected
      } else {
        next.add(key);
      }
      return next;
    });
  };

  const toggleExpand = (id: string) => {
    setExpandedIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  // Determine the correct hitter_stats key based on pitcher hand
  const pitcherHandChar = (pitcherHand ?? "R").toUpperCase().startsWith("L") ? "L" : "R";
  const hitterStatsKey = pitcherHandChar === "L" ? "vs_lhp" : "vs_rhp";

  if (batters.length === 0) return null;

  return (
    <View style={s.handSection}>
      <Text style={s.handLabel}>{label}</Text>

      {/* Pitch toggles */}
      {pitcherMix.length > 0 ? (
        <ScrollView horizontal showsHorizontalScrollIndicator={false} style={s.pitchToggleRow}>
          <View style={s.pitchToggleInner}>
            {pitcherMix.map((pitch, idx) => (
              <PitchToggle
                key={`${normPitch(pitch.pitch_name)}-${idx}`}
                pitch={pitch}
                active={selectedPitches.has(normPitch(pitch.pitch_name))}
                onToggle={() => togglePitch(pitch.pitch_name ?? "")}
              />
            ))}
          </View>
        </ScrollView>
      ) : null}

      {/* Column headers */}
      <View style={s.headerRow}>
        <View style={s.headerInfoCol}>
          <Text style={s.headerText}>PLAYER</Text>
        </View>
        <ScrollView horizontal showsHorizontalScrollIndicator={false} style={s.statsScroll}>
          <View style={s.statsRow}>
            <Text style={s.headerCell}>AVG</Text>
            <Text style={s.headerCell}>ISO</Text>
            <Text style={s.headerCell}>BRL%</Text>
            <Text style={s.headerCell}>HH%</Text>
            <Text style={s.headerCell}>SLG</Text>
          </View>
        </ScrollView>
      </View>

      {/* Batter rows */}
      {batters.map((batter) => {
        const batterId = String(batter.batter_id ?? batter.batter_name ?? "");
        const pitchRows: MlbBatterVsPitchRow[] =
          (batter as any).hitter_stats_vs_pitches?.[hitterStatsKey] ?? [];
        const stats = aggregateStatsForPitches(pitchRows, selectedPitches, batter);

        return (
          <BatterRow
            key={batterId}
            batter={batter}
            stats={stats}
            expanded={expandedIds.has(batterId)}
            onToggleExpand={() => toggleExpand(batterId)}
          />
        );
      })}
    </View>
  );
}

// ── Main screen ─────────────────────────────────────────────────────────────

export function MlbHrMatchupScreen() {
  const { colors } = useTheme();
  const router = useRouter();
  const params = useLocalSearchParams<{ gamePk?: string; awayTeam?: string; homeTeam?: string }>();
  const gamePk = Number(params.gamePk);
  const { data, loading, error, refetch } = useMlbMatchupDetail(Number.isFinite(gamePk) ? gamePk : null);

  const game = data?.game;
  const awayTeam = params.awayTeam ?? game?.away_team ?? "Away";
  const homeTeam = params.homeTeam ?? game?.home_team ?? "Home";
  const awayLogo = getMlbTeamLogo(awayTeam);
  const homeLogo = getMlbTeamLogo(homeTeam);

  return (
    <ScrollView style={s.screen} contentContainerStyle={s.content}>
      {/* ── Navigation ── */}
      <View style={s.navRow}>
        <Pressable onPress={() => router.push("/(tabs)/mlb" as any)} style={s.navBtn}>
          <Text style={s.navBtnText}>← MLB</Text>
        </Pressable>
        <Pressable onPress={() => router.push("/(tabs)/home")} style={s.navBtn}>
          <Text style={s.navBtnText}>Home</Text>
        </Pressable>
      </View>

      {/* ── Sub-tab indicator ── */}
      <View style={s.tabRow}>
        <Pressable
          onPress={() =>
            router.push({
              pathname: "/(tabs)/mlb/match/[gamePk]" as any,
              params: { gamePk: String(gamePk), homeTeam, awayTeam },
            })
          }
          style={s.tabInactive}
        >
          <Text style={s.tabTextInactive}>Home Runs</Text>
        </Pressable>
        <View style={s.tabActive}>
          <Text style={s.tabTextActive}>HR Matchup</Text>
        </View>
        <Pressable
          onPress={() =>
            router.push({
              pathname: "/(tabs)/mlb/pitching-props/[gamePk]" as any,
              params: { gamePk: String(gamePk), homeTeam, awayTeam },
            })
          }
          style={s.tabInactive}
        >
          <Text style={s.tabTextInactive}>Pitching</Text>
        </Pressable>
        <Pressable
          onPress={() =>
            router.push({
              pathname: "/(tabs)/mlb/lineup-matchup/[gamePk]" as any,
              params: { gamePk: String(gamePk), homeTeam, awayTeam },
            })
          }
          style={s.tabInactive}
        >
          <Text style={s.tabTextInactive}>Lineup</Text>
        </Pressable>
      </View>

      {/* ── Hero ── */}
      <View style={[s.hero, { borderColor: colors.border.subtle }]}>
        <Text style={s.eyebrow}>HR MATCHUP — BATTER vs PITCH MIX</Text>
        <View style={s.slugRow}>
          <View style={s.heroTeamCol}>
            {awayLogo ? <Image source={{ uri: awayLogo }} style={s.heroLogo} /> : <View style={s.heroLogo} />}
            <Text style={s.heroTeamName} numberOfLines={2}>{awayTeam}</Text>
          </View>
          <View style={s.heroCenterCol}>
            <Text style={s.slugTime}>
              {game?.start_time_utc ? new Date(game.start_time_utc).toLocaleTimeString() : "TBD"}
            </Text>
            <Text style={s.slugMeta}>{game?.venue_name ?? "Venue TBD"}</Text>
          </View>
          <View style={s.heroTeamCol}>
            {homeLogo ? <Image source={{ uri: homeLogo }} style={s.heroLogo} /> : <View style={s.heroLogo} />}
            <Text style={s.heroTeamName} numberOfLines={2}>{homeTeam}</Text>
          </View>
        </View>
        <View style={s.tagRow}>
          <View style={s.pill}>
            <Text style={s.pillText}>
              {game?.weather?.weather_indicator ?? "Weather"}{" "}
              {game?.weather?.game_temp != null ? `${Math.round(game.weather.game_temp)}°` : ""}
              {game?.weather?.wind_speed != null
                ? ` · ${Math.round(game.weather.wind_speed)} mph ${game?.weather?.wind_direction_label ?? ""}`
                : ""}
            </Text>
          </View>
          <View style={s.pill}>
            <Text style={s.pillText}>
              ML {fmtOdds(game?.odds?.away_moneyline)} / {fmtOdds(game?.odds?.home_moneyline)}
              {game?.odds?.over_under != null ? ` · O/U ${game.odds.over_under}` : ""}
            </Text>
          </View>
        </View>
      </View>

      {loading ? <ActivityIndicator color="#93C5FD" /> : null}

      {error ? (
        <Pressable onPress={refetch} style={[s.errorBox, { borderColor: colors.border.subtle }]}>
          <Text style={s.errorTitle}>Failed to load matchup.</Text>
          <Text style={s.errorText}>{error}</Text>
          <Text style={s.errorRetry}>Tap to retry</Text>
        </Pressable>
      ) : null}

      {/* ── Pitcher panels ── */}
      {(data?.pitchers ?? []).map((pitcher) => {
        const batters = dedupeBatters(pitcher.batters ?? []).slice(0, 12);
        const pitcherHandRaw = (pitcher.pitcher_hand ?? "R").toUpperCase();

        // Determine which pitch mix to show per handedness section
        const mixVsRhb = pitcher.pitch_mix?.vs_rhb ?? [];
        const mixVsLhb = pitcher.pitch_mix?.vs_lhb ?? [];

        // Split batters by handedness
        const rhBatters = batters.filter((b) => {
          const side = (b.bat_side ?? "").toUpperCase();
          return side === "R" || side === "S";
        });
        const lhBatters = batters.filter((b) => {
          const side = (b.bat_side ?? "").toUpperCase();
          return side === "L" || side === "S";
        });

        return (
          <View key={String(pitcher.pitcher_id)} style={[s.panel, { borderColor: colors.border.subtle }]}>
            <Text style={s.sectionEyebrow}>{pitcher.offense_team ?? "OFFENSE"} HITTERS</Text>
            <Text style={s.sectionTitle}>
              vs {pitcher.pitcher_name ?? "Pitcher"}{" "}
              <Text style={s.sectionHand}>{pitcherHandRaw}</Text>
            </Text>

            <HandednessSection
              label={`vs RHB · ${pitcher.pitcher_name ?? "Pitcher"} Pitch Mix`}
              batters={rhBatters}
              pitcherMix={mixVsRhb}
              pitcherHand={pitcherHandRaw}
            />

            <HandednessSection
              label={`vs LHB · ${pitcher.pitcher_name ?? "Pitcher"} Pitch Mix`}
              batters={lhBatters}
              pitcherMix={mixVsLhb}
              pitcherHand={pitcherHandRaw}
            />

            {!batters.length ? (
              <Text style={s.emptyText}>No hitter data for this pitcher.</Text>
            ) : null}
          </View>
        );
      })}

      {!loading && !error && !(data?.pitchers?.length) ? (
        <View style={[s.panel, { borderColor: colors.border.subtle }]}>
          <Text style={s.emptyTitle}>No MLB model data for this game yet.</Text>
          <Text style={s.emptyText}>Data will populate once the model run completes.</Text>
        </View>
      ) : null}
    </ScrollView>
  );
}

// ── Styles ──────────────────────────────────────────────────────────────────

const STAT_CELL_W = 58;

const s = StyleSheet.create({
  screen: { flex: 1, backgroundColor: "#050A18" },
  content: { padding: 16, gap: 10, paddingBottom: 40 },

  // Nav
  navRow: { flexDirection: "row", gap: 8, marginBottom: 2 },
  navBtn: {
    borderWidth: StyleSheet.hairlineWidth,
    borderColor: "#334155",
    borderRadius: 8,
    paddingHorizontal: 12,
    paddingVertical: 6,
    backgroundColor: "#0F172A",
  },
  navBtnText: { color: "#93C5FD", fontSize: 12, fontWeight: "700" },

  // Tabs
  tabRow: { flexDirection: "row", gap: 0, marginBottom: 4 },
  tabActive: { flex: 1, borderBottomWidth: 2, borderBottomColor: "#10B981", paddingVertical: 10, alignItems: "center" },
  tabInactive: { flex: 1, borderBottomWidth: 2, borderBottomColor: "#1E293B", paddingVertical: 10, alignItems: "center" },
  tabTextActive: { color: "#10B981", fontSize: 12, fontWeight: "800" },
  tabTextInactive: { color: "#64748B", fontSize: 12, fontWeight: "700" },

  // Hero
  hero: { borderWidth: StyleSheet.hairlineWidth, borderRadius: 16, backgroundColor: "#071731", padding: 16, gap: 8 },
  eyebrow: { color: "#10B981", fontSize: 11, fontWeight: "700" },
  slugRow: { flexDirection: "row", alignItems: "center", justifyContent: "space-between", gap: 12 },
  heroTeamCol: { flex: 1.25, alignItems: "center", gap: 6 },
  heroCenterCol: { flex: 1.8, alignItems: "center", gap: 3 },
  heroLogo: { width: 36, height: 36, borderRadius: 18, backgroundColor: "#111827" },
  heroTeamName: { color: "#E5E7EB", fontSize: 14, fontWeight: "800", textAlign: "center" },
  slugTime: { color: "#F8FAFC", fontSize: 18, fontWeight: "800" },
  slugMeta: { color: "#A7C0E8", fontSize: 11 },
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

  // Panel
  panel: { borderWidth: StyleSheet.hairlineWidth, borderRadius: 14, backgroundColor: "#0B1529", padding: 12, gap: 8 },
  sectionEyebrow: { color: "#64748B", fontSize: 11, fontWeight: "700" },
  sectionTitle: { color: "#E5E7EB", fontSize: 16, fontWeight: "800" },
  sectionHand: { color: "#94A3B8", fontSize: 13, fontWeight: "600" },

  // Handedness section
  handSection: { gap: 4, marginTop: 8 },
  handLabel: { color: "#10B981", fontSize: 12, fontWeight: "800", marginBottom: 4 },

  // Pitch toggles
  pitchToggleRow: { marginBottom: 6 },
  pitchToggleInner: { flexDirection: "row", gap: 6 },
  pitchToggle: {
    borderWidth: 1,
    borderRadius: 999,
    paddingHorizontal: 10,
    paddingVertical: 5,
  },
  pitchToggleActive: {
    borderColor: "#10B981",
    backgroundColor: "rgba(16,185,129,0.18)",
  },
  pitchToggleInactive: {
    borderColor: "#334155",
    backgroundColor: "#0F172A",
  },
  pitchToggleText: { color: "#E5E7EB", fontSize: 11, fontWeight: "700" },
  pitchToggleTextActive: { color: "#A7F3D0" },
  pitchTogglePct: { color: "#64748B", fontSize: 10, fontWeight: "600" },

  // Header row
  headerRow: {
    flexDirection: "row",
    borderBottomWidth: StyleSheet.hairlineWidth,
    borderBottomColor: "#334155",
    paddingBottom: 4,
    marginBottom: 2,
  },
  headerInfoCol: { width: 130, paddingLeft: 4 },
  headerText: { color: "#64748B", fontSize: 9, fontWeight: "800" },
  headerCell: {
    width: STAT_CELL_W,
    color: "#64748B",
    fontSize: 9,
    fontWeight: "800",
    textAlign: "center",
  },

  // Batter row
  batterRowWrap: {
    borderBottomWidth: StyleSheet.hairlineWidth,
    borderBottomColor: "rgba(51,65,85,0.5)",
  },
  batterRow: {
    flexDirection: "row",
    alignItems: "center",
    minHeight: 36,
  },
  batterInfoCol: {
    width: 130,
    flexDirection: "row",
    alignItems: "center",
    gap: 4,
    paddingLeft: 2,
    paddingVertical: 4,
  },
  chevron: { color: "#64748B", fontSize: 11, width: 14 },
  gradeChip: {
    borderWidth: 1,
    borderRadius: 4,
    width: 22,
    height: 18,
    alignItems: "center",
    justifyContent: "center",
  },
  gradeChipText: { fontSize: 9, fontWeight: "900" },
  nameWrap: { flex: 1, gap: 0 },
  batterName: { color: "#E5E7EB", fontSize: 11, fontWeight: "700" },
  batterMeta: { color: "#64748B", fontSize: 9, fontWeight: "600" },

  // Stats
  statsScroll: { flex: 1 },
  statsRow: { flexDirection: "row" },
  statCell: {
    width: STAT_CELL_W,
    color: "#E2E8F0",
    fontSize: 11,
    fontWeight: "700",
    textAlign: "center",
    paddingVertical: 6,
  },
  cellGreen: { color: "#34D399" },

  // Expanded
  expandedArea: {
    paddingHorizontal: 10,
    paddingVertical: 8,
    backgroundColor: "rgba(15,23,42,0.4)",
    borderTopWidth: StyleSheet.hairlineWidth,
    borderTopColor: "#1E293B",
    gap: 6,
  },
  expandedTitle: { color: "#94A3B8", fontSize: 10, fontWeight: "800" },
  bvpWrap: { gap: 4 },
  bvpSummary: { flexDirection: "row", gap: 8 },
  bvpSummaryText: { color: "#CBD5E1", fontSize: 11, fontWeight: "700" },
  bvpTable: {
    borderWidth: StyleSheet.hairlineWidth,
    borderColor: "#334155",
    borderRadius: 6,
    overflow: "hidden",
  },
  bvpHeaderRow: { flexDirection: "row", backgroundColor: "#0F172A" },
  bvpHeaderCell: {
    flex: 1,
    color: "#64748B",
    fontSize: 9,
    fontWeight: "800",
    textAlign: "center",
    paddingVertical: 5,
  },
  bvpValueRow: { flexDirection: "row", backgroundColor: "rgba(15,23,42,0.25)" },
  bvpValueCell: {
    flex: 1,
    color: "#E2E8F0",
    fontSize: 11,
    fontWeight: "700",
    textAlign: "center",
    paddingVertical: 6,
  },
  bvpEmpty: { color: "#475569", fontSize: 11, fontStyle: "italic" },

  // Batted ball profile
  bbWrap: { gap: 6, marginTop: 6 },
  bbBarsContainer: { gap: 6 },
  bbBarsRow: { flexDirection: "row", gap: 8, flexWrap: "wrap" },
  miniBarWrap: { width: 90, gap: 2 },
  miniBarLabel: { color: "#64748B", fontSize: 8, fontWeight: "800" },
  miniBarTrack: {
    height: 6,
    borderRadius: 3,
    backgroundColor: "#1E293B",
    overflow: "hidden",
  },
  miniBarFill: { height: 6, borderRadius: 3 },
  miniBarValue: { color: "#CBD5E1", fontSize: 9, fontWeight: "700" },

  // Hit log table
  logHeaderRow: {
    flexDirection: "row",
    backgroundColor: "#0F172A",
    borderBottomWidth: StyleSheet.hairlineWidth,
    borderBottomColor: "#334155",
  },
  logHeaderCell: {
    width: 52,
    color: "#64748B",
    fontSize: 8,
    fontWeight: "800",
    textAlign: "center",
    paddingVertical: 5,
  },
  logDateCol: { width: 72, textAlign: "left", paddingLeft: 4 },
  logPitchCol: { width: 90, textAlign: "left", paddingLeft: 4 },
  logTrajCol: { width: 80, textAlign: "left", paddingLeft: 4 },
  logResultCol: { width: 76, textAlign: "left", paddingLeft: 4 },
  logRow: {
    flexDirection: "row",
    borderBottomWidth: StyleSheet.hairlineWidth,
    borderBottomColor: "rgba(51,65,85,0.5)",
  },
  logCell: {
    width: 52,
    color: "#E2E8F0",
    fontSize: 10,
    fontWeight: "600",
    textAlign: "center",
    paddingVertical: 5,
  },

  // Error / empty
  errorBox: { borderWidth: StyleSheet.hairlineWidth, borderRadius: 12, backgroundColor: "#1F2937", padding: 12 },
  errorTitle: { color: "#FCA5A5", fontWeight: "700" },
  errorText: { color: "#FECACA", marginTop: 4, fontSize: 12 },
  errorRetry: { color: "#E5E7EB", marginTop: 8, fontSize: 12 },
  emptyTitle: { color: "#E5E7EB", fontWeight: "700" },
  emptyText: { color: "#A7C0E8", marginTop: 6, fontSize: 12 },
});

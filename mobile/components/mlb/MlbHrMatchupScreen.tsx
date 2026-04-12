import {
  ActivityIndicator,
  Image,
  Linking,
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
  useMlbBattingOrder,
  type MlbBatterPick,
  type MlbPitchMixRow,
  type MlbBatterVsPitchRow,
} from "@/hooks/mlb/useMlbMatchups";
import { useTheme } from "@/store/useTheme";
import { getMlbTeamLogo } from "@/utils/mlbLogos";
import { usePropBetslip } from "@/store/usePropBetslip";
import { buildFanDuelParlay, buildDraftKingsParlay, getBuildPlatform } from "@/utils/parlayBuilder";

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

function normPitch(name?: string | null): string {
  return (name ?? "").trim().toLowerCase();
}

// ── Heat-map cell background color ─────────────────────────────────────────

function heatBg(metric: string, value?: number | null): string {
  if (value == null || !Number.isFinite(value)) return "transparent";
  switch (metric) {
    case "avg":
      if (value >= 0.300) return "rgba(22,163,74,0.55)";
      if (value >= 0.260) return "rgba(22,163,74,0.28)";
      if (value <= 0.180) return "rgba(220,38,38,0.50)";
      if (value <= 0.220) return "rgba(220,38,38,0.25)";
      return "transparent";
    case "obp":
      if (value >= 0.370) return "rgba(22,163,74,0.55)";
      if (value >= 0.330) return "rgba(22,163,74,0.28)";
      if (value <= 0.260) return "rgba(220,38,38,0.50)";
      if (value <= 0.300) return "rgba(220,38,38,0.25)";
      return "transparent";
    case "slg":
      if (value >= 0.500) return "rgba(22,163,74,0.55)";
      if (value >= 0.430) return "rgba(22,163,74,0.28)";
      if (value <= 0.300) return "rgba(220,38,38,0.50)";
      if (value <= 0.370) return "rgba(220,38,38,0.25)";
      return "transparent";
    case "iso":
      if (value >= 0.220) return "rgba(22,163,74,0.55)";
      if (value >= 0.170) return "rgba(22,163,74,0.28)";
      if (value <= 0.080) return "rgba(220,38,38,0.50)";
      if (value <= 0.120) return "rgba(220,38,38,0.25)";
      return "transparent";
    case "woba":
      if (value >= 0.380) return "rgba(22,163,74,0.55)";
      if (value >= 0.340) return "rgba(22,163,74,0.28)";
      if (value <= 0.270) return "rgba(220,38,38,0.50)";
      if (value <= 0.310) return "rgba(220,38,38,0.25)";
      return "transparent";
    case "k_pct":
      // Lower K% is better for the batter (inverted: green=low, red=high)
      if (value <= 12) return "rgba(22,163,74,0.55)";
      if (value <= 18) return "rgba(22,163,74,0.28)";
      if (value >= 35) return "rgba(220,38,38,0.50)";
      if (value >= 28) return "rgba(220,38,38,0.25)";
      return "transparent";
    case "barrel_pct":
      if (value >= 15) return "rgba(22,163,74,0.55)";
      if (value >= 10) return "rgba(22,163,74,0.28)";
      if (value <= 3) return "rgba(220,38,38,0.50)";
      if (value <= 6) return "rgba(220,38,38,0.25)";
      return "transparent";
    case "hh_pct":
      if (value >= 45) return "rgba(22,163,74,0.55)";
      if (value >= 38) return "rgba(22,163,74,0.28)";
      if (value <= 25) return "rgba(220,38,38,0.50)";
      if (value <= 30) return "rgba(220,38,38,0.25)";
      return "transparent";
    case "avg_ev":
      if (value >= 92) return "rgba(22,163,74,0.55)";
      if (value >= 89) return "rgba(22,163,74,0.28)";
      if (value <= 84) return "rgba(220,38,38,0.50)";
      if (value <= 86) return "rgba(220,38,38,0.25)";
      return "transparent";
    case "fb_pct":
      if (value >= 45) return "rgba(22,163,74,0.55)";
      if (value >= 38) return "rgba(22,163,74,0.28)";
      if (value <= 20) return "rgba(220,38,38,0.50)";
      if (value <= 28) return "rgba(220,38,38,0.25)";
      return "transparent";
  }
  return "transparent";
}

// ── Aggregate batter stats for selected pitches ─────────────────────────────

type AggregatedStats = {
  avg: number | null;
  obp: number | null;
  slg: number | null;
  iso: number | null;
  woba: number | null;
  k_pct: number | null;
  pa: number | null;
  hits: number | null;
  at_bats: number | null;
  hr: number | null;
  avg_ev: number | null;
  barrel_pct_l15: number | null;
  hh_pct: number | null;
  fb_pct: number | null;
};

function aggregateStatsForPitches(
  pitchRows: MlbBatterVsPitchRow[],
  selectedPitches: Set<string>,
  batter: MlbBatterPick,
  pitcherMixRows?: MlbPitchMixRow[],
): AggregatedStats {
  const filtered = pitchRows.filter((r) => selectedPitches.has(normPitch(r.pitch_name)));
  if (filtered.length === 0) {
    return {
      avg: null, obp: null,
      iso: batter.iso ?? null,
      barrel_pct_l15: batter.l15_barrel_pct ?? null,
      hh_pct: batter.l15_hard_hit_pct ?? null,
      slg: batter.slg ?? null,
      woba: null, k_pct: null,
      pa: null, hits: null, at_bats: null,
      hr: null,
      avg_ev: batter.season_ev ?? batter.l15_ev ?? null,
      fb_pct: (batter as any).bvp_batted_ball?.profile?.fb_pct ?? null,
    };
  }

  let totalCount = 0, totalHits = 0, totalHr = 0;
  let sumBa = 0, sumIso = 0, sumSlg = 0, sumWoba = 0;
  let hasBa = false, hasIso = false, hasSlg = false, hasWoba = false;

  for (const row of filtered) {
    const count = row.pitch_count ?? row.count ?? 1;
    totalCount += count;
    totalHits += row.hits ?? 0;
    totalHr += row.hr ?? 0;
    if (row.ba != null) { sumBa += row.ba * count; hasBa = true; }
    if (row.iso != null) { sumIso += row.iso * count; hasIso = true; }
    if (row.slg != null) { sumSlg += row.slg * count; hasSlg = true; }
    if (row.woba != null) { sumWoba += row.woba * count; hasWoba = true; }
  }

  // Compute weighted K% from pitcher pitch mix for the selected pitches
  let kPct: number | null = null;
  if (pitcherMixRows && pitcherMixRows.length > 0) {
    const mixFiltered = pitcherMixRows.filter((r) => selectedPitches.has(normPitch(r.pitch_name)));
    let sumK = 0, sumMixCount = 0;
    let hasK = false;
    for (const m of mixFiltered) {
      const mc = m.pitch_count ?? 1;
      if (m.k_pct != null) { sumK += m.k_pct * mc; sumMixCount += mc; hasK = true; }
    }
    if (hasK && sumMixCount > 0) kPct = sumK / sumMixCount;
  }

  const w = totalCount || 1;
  const avg = hasBa ? sumBa / w : null;
  const slgVal = hasSlg ? sumSlg / w : (batter.slg ?? null);
  return {
    avg,
    obp: null,
    iso: hasIso ? sumIso / w : (batter.iso ?? null),
    barrel_pct_l15: batter.l15_barrel_pct ?? null,
    hh_pct: batter.l15_hard_hit_pct ?? null,
    slg: slgVal,
    woba: hasWoba ? sumWoba / w : null,
    k_pct: kPct,
    pa: totalCount > 0 ? totalCount : null,
    hits: totalHits > 0 ? totalHits : null,
    at_bats: totalCount > 0 ? totalCount : null,
    hr: totalHr > 0 ? totalHr : null,
    avg_ev: batter.season_ev ?? batter.l15_ev ?? null,
    fb_pct: (batter as any).bvp_batted_ball?.profile?.fb_pct ?? null,
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

// ── Heat-map stat cell ─────────────────────────────────────────────────────

function StatCell({ metric, value, display }: { metric: string; value?: number | null; display: string }) {
  return (
    <View style={[s.statCell, { backgroundColor: heatBg(metric, value) }]}>
      <Text style={s.statCellText}>{display}</Text>
    </View>
  );
}

// ── Pitch pill color map (matches PropFinder) ───────────────────────────────

const PITCH_COLORS: Record<string, string> = {
  "four-seam fb": "#EF4444",
  "4-seam fastball": "#EF4444",
  "fastball": "#EF4444",
  "sinker": "#F97316",
  "cutter": "#A855F7",
  "slider": "#EAB308",
  "sweeper": "#EAB308",
  "curveball": "#22D3EE",
  "changeup": "#10B981",
  "splitter": "#6366F1",
  "knuckle curve": "#06B6D4",
  "slurve": "#84CC16",
};

function pitchColor(name?: string | null): string {
  const n = (name ?? "").trim().toLowerCase();
  for (const [key, color] of Object.entries(PITCH_COLORS)) {
    if (n.includes(key) || key.includes(n)) return color;
  }
  return "#64748B";
}

// ── H-AB format helper ──────────────────────────────────────────────────────

function fmtHAB(hits?: number | null, ab?: number | null): string {
  if (hits == null || ab == null) return "—";
  return `${hits}-${ab}`;
}

// ── Handedness section (vs RHB or vs LHB) ───────────────────────────────────

function HandednessSection({
  label,
  pitcherName,
  batters,
  pitcherMix,
  pitcherHand,
  weakSpotIds,
  selectedKeys,
  onToggleSelect,
}: {
  label: string;
  pitcherName: string;
  batters: MlbBatterPick[];
  pitcherMix: MlbPitchMixRow[];
  pitcherHand: string;
  weakSpotIds: Set<number>;
  selectedKeys?: Set<string>;
  onToggleSelect?: (batterId: string) => void;
}) {
  const [selectedPitches, setSelectedPitches] = useState<Set<string>>(() => {
    const over25 = pitcherMix.filter((r) => (r.pitch_pct ?? 0) >= 25);
    const selected = over25.length > 0 ? over25 : pitcherMix;
    return new Set(selected.map((r) => normPitch(r.pitch_name)));
  });

  const togglePitch = (pitchName: string) => {
    setSelectedPitches((prev) => {
      const next = new Set(prev);
      const key = normPitch(pitchName);
      if (next.has(key)) {
        if (next.size > 1) next.delete(key);
      } else {
        next.add(key);
      }
      return next;
    });
  };

  const clearPitches = () => {
    setSelectedPitches(new Set(pitcherMix.map((r) => normPitch(r.pitch_name))));
  };

  const pitcherHandChar = (pitcherHand ?? "R").toUpperCase().startsWith("L") ? "L" : "R";
  const hitterStatsKey = pitcherHandChar === "L" ? "vs_lhp" : "vs_rhp";

  // Compute stats for each batter
  const batterStats = useMemo(() => {
    return batters.map((batter) => {
      const pitchRows: MlbBatterVsPitchRow[] =
        (batter as any).hitter_stats_vs_pitches?.[hitterStatsKey] ?? [];
      return {
        batter,
        stats: aggregateStatsForPitches(pitchRows, selectedPitches, batter, pitcherMix),
      };
    });
  }, [batters, selectedPitches, hitterStatsKey, pitcherMix]);

  // Compute averages row
  const avgRow = useMemo(() => {
    let count = 0, sumAvg = 0, sumSlg = 0, sumIso = 0, sumWoba = 0, sumKPct = 0;
    let sumEv = 0, sumBarrel = 0, sumHh = 0, sumFb = 0;
    let hasAvg = false, hasSlg = false, hasIso = false, hasWoba = false, hasK = false;
    let hasEv = false, hasBarrel = false, hasHh = false, hasFb = false;
    let totalPa = 0, totalHits = 0, totalAb = 0, totalHr = 0;
    for (const { stats } of batterStats) {
      count++;
      if (stats.avg != null) { sumAvg += stats.avg; hasAvg = true; }
      if (stats.slg != null) { sumSlg += stats.slg; hasSlg = true; }
      if (stats.iso != null) { sumIso += stats.iso; hasIso = true; }
      if (stats.woba != null) { sumWoba += stats.woba; hasWoba = true; }
      if (stats.k_pct != null) { sumKPct += stats.k_pct; hasK = true; }
      if (stats.avg_ev != null) { sumEv += stats.avg_ev; hasEv = true; }
      if (stats.barrel_pct_l15 != null) { sumBarrel += stats.barrel_pct_l15; hasBarrel = true; }
      if (stats.hh_pct != null) { sumHh += stats.hh_pct; hasHh = true; }
      if (stats.fb_pct != null) { sumFb += stats.fb_pct; hasFb = true; }
      if (stats.pa != null) totalPa += stats.pa;
      if (stats.hits != null) totalHits += stats.hits;
      if (stats.at_bats != null) totalAb += stats.at_bats;
      if (stats.hr != null) totalHr += stats.hr;
    }
    const n = count || 1;
    return {
      pa: totalPa || null,
      hits: totalHits || null,
      at_bats: totalAb || null,
      avg: hasAvg ? sumAvg / n : null,
      obp: null as number | null,
      slg: hasSlg ? sumSlg / n : null,
      iso: hasIso ? sumIso / n : null,
      woba: hasWoba ? sumWoba / n : null,
      k_pct: hasK ? sumKPct / n : null,
      hr: totalHr || null,
      avg_ev: hasEv ? sumEv / n : null,
      barrel_pct: hasBarrel ? sumBarrel / n : null,
      hh_pct: hasHh ? sumHh / n : null,
      fb_pct: hasFb ? sumFb / n : null,
    };
  }, [batterStats]);

  if (batters.length === 0) return null;

  // Build pitch label from active pitches
  const activePitchNames = pitcherMix
    .filter((p) => selectedPitches.has(normPitch(p.pitch_name)))
    .map((p) => p.pitch_name)
    .join(", ");

  return (
    <View style={s.handSection}>
      {/* Section header with pitch names */}
      <Text style={s.handSectionTitle}>
        {label} · {activePitchNames || "All Pitches"}
      </Text>

      {/* Pitch toggle pills – PropFinder colored style */}
      {pitcherMix.length > 0 ? (
        <View style={s.pitchToggleWrap}>
          <ScrollView horizontal showsHorizontalScrollIndicator={false} style={s.pitchToggleRow}>
            <View style={s.pitchToggleInner}>
              {pitcherMix.map((pitch, idx) => {
                const active = selectedPitches.has(normPitch(pitch.pitch_name));
                const color = pitchColor(pitch.pitch_name);
                return (
                  <Pressable
                    key={`${normPitch(pitch.pitch_name)}-${idx}`}
                    style={[
                      s.pitchPill,
                      active
                        ? { borderColor: color, backgroundColor: `${color}33` }
                        : s.pitchPillInactive,
                    ]}
                    onPress={() => togglePitch(pitch.pitch_name ?? "")}
                  >
                    <Text
                      style={[
                        s.pitchPillText,
                        active ? { color } : null,
                      ]}
                      numberOfLines={1}
                    >
                      {pitch.pitch_name ?? "?"} {pitch.pitch_pct != null ? `${pitch.pitch_pct.toFixed(0)}%` : ""}
                    </Text>
                  </Pressable>
                );
              })}
            </View>
          </ScrollView>
          <Pressable style={s.pitchClearBtn} onPress={clearPitches}>
            <Text style={s.pitchClearText}>Clear</Text>
          </Pressable>
        </View>
      ) : null}

      {/* Table – matches PropFinder column order */}
      <ScrollView horizontal showsHorizontalScrollIndicator={false}>
        <View>
          {/* Column headers */}
          <View style={s.tableHeaderRow}>
            {onToggleSelect ? <View style={s.checkCol}><Text style={s.colHeader}>+</Text></View> : null}
            <View style={s.rankCol}><Text style={s.colHeader}>#</Text></View>
            <View style={s.playerCol}><Text style={[s.colHeader, { textAlign: "left" }]}>PLAYER</Text></View>
            <View style={s.paCol}><Text style={s.colHeader}>PA</Text></View>
            <View style={s.habCol}><Text style={s.colHeader}>H-AB</Text></View>
            <View style={s.hrCol}><Text style={s.colHeader}>HR</Text></View>
            <View style={s.statColHdr}><Text style={s.colHeader}>AVG</Text></View>
            <View style={s.statColHdr}><Text style={s.colHeader}>ISO</Text></View>
            <View style={s.statColHdr}><Text style={[s.colHeader, { fontSize: 8 }]}>BARREL%</Text></View>
            <View style={s.statColHdr}><Text style={s.colHeader}>HH%</Text></View>
            <View style={s.statColHdr}><Text style={s.colHeader}>FB%</Text></View>
            <View style={s.statColHdr}><Text style={s.colHeader}>AVG EV</Text></View>
            <View style={s.statColHdr}><Text style={s.colHeader}>SLG</Text></View>
            <View style={s.statColHdr}><Text style={s.colHeader}>wOBA</Text></View>
            <View style={s.statColHdr}><Text style={s.colHeader}>K%</Text></View>
            <View style={s.statColHdr}><Text style={s.colHeader}>OBP</Text></View>
          </View>

          {/* Batter rows */}
          {batterStats.map(({ batter, stats }, idx) => {
            const isWeakSpot = batter.batter_id != null && weakSpotIds.has(batter.batter_id);
            const batterId = String(batter.batter_id ?? batter.batter_name ?? "");
            const isSelected = selectedKeys?.has(batterId) ?? false;
            return (
              <View
                key={String(batter.batter_id ?? batter.batter_name ?? idx)}
                style={[s.tableRow, isWeakSpot ? s.tableRowWeak : null, isSelected ? s.tableRowSelected : null]}
              >
                {onToggleSelect ? (
                  <Pressable style={s.checkCol} onPress={() => onToggleSelect(batterId)}>
                    <Text style={isSelected ? s.checkOn : s.checkOff}>{isSelected ? "☑" : "☐"}</Text>
                  </Pressable>
                ) : null}
                <View style={s.rankCol}>
                  <Text style={s.playerRank}>{idx + 1}</Text>
                </View>
                <View style={s.playerCol}>
                  <View style={s.playerNameWrap}>
                    <Text style={s.playerName} numberOfLines={1}>
                      {isWeakSpot ? "🎯 " : ""}{batter.batter_name ?? "—"}
                    </Text>
                    <Text style={s.playerMeta}>{handLabel(batter.bat_side)}</Text>
                  </View>
                </View>
                <View style={s.paCol}>
                  <Text style={s.paCellText}>{stats.pa ?? "—"}</Text>
                </View>
                <View style={s.habCol}>
                  <Text style={s.paCellText}>{fmtHAB(stats.hits, stats.at_bats)}</Text>
                </View>
                <View style={s.hrCol}>
                  <Text style={s.paCellText}>{stats.hr ?? "—"}</Text>
                </View>
                <StatCell metric="avg" value={stats.avg} display={fmt(stats.avg)} />
                <StatCell metric="iso" value={stats.iso} display={fmt(stats.iso)} />
                <StatCell metric="barrel_pct" value={stats.barrel_pct_l15} display={fmtPct(stats.barrel_pct_l15)} />
                <StatCell metric="hh_pct" value={stats.hh_pct} display={fmtPct(stats.hh_pct)} />
                <StatCell metric="fb_pct" value={stats.fb_pct} display={fmtPct(stats.fb_pct)} />
                <StatCell metric="avg_ev" value={stats.avg_ev} display={stats.avg_ev != null ? stats.avg_ev.toFixed(1) : "—"} />
                <StatCell metric="slg" value={stats.slg} display={fmt(stats.slg)} />
                <StatCell metric="woba" value={stats.woba} display={fmt(stats.woba)} />
                <StatCell metric="k_pct" value={stats.k_pct} display={stats.k_pct != null ? `${stats.k_pct.toFixed(0)}%` : "—"} />
                <StatCell metric="obp" value={stats.obp} display={fmt(stats.obp)} />
              </View>
            );
          })}

          {/* Averages row */}
          {batterStats.length > 0 ? (
            <View style={s.avgRow}>
              {onToggleSelect ? <View style={s.checkCol} /> : null}
              <View style={s.rankCol} />
              <View style={s.playerCol}>
                <Text style={s.avgLabel}>
                  {label.startsWith("vs RHB") ? "RHB" : "LHB"} Avg
                </Text>
              </View>
              <View style={s.paCol}>
                <Text style={s.avgCellText}>{avgRow.pa ?? "—"}</Text>
              </View>
              <View style={s.habCol}>
                <Text style={s.avgCellText}>{fmtHAB(avgRow.hits, avgRow.at_bats)}</Text>
              </View>
              <View style={s.hrCol}>
                <Text style={s.avgCellText}>{avgRow.hr ?? "—"}</Text>
              </View>
              <StatCell metric="avg" value={avgRow.avg} display={fmt(avgRow.avg)} />
              <StatCell metric="iso" value={avgRow.iso} display={fmt(avgRow.iso)} />
              <StatCell metric="barrel_pct" value={avgRow.barrel_pct} display={fmtPct(avgRow.barrel_pct)} />
              <StatCell metric="hh_pct" value={avgRow.hh_pct} display={fmtPct(avgRow.hh_pct)} />
              <StatCell metric="fb_pct" value={avgRow.fb_pct} display={fmtPct(avgRow.fb_pct)} />
              <StatCell metric="avg_ev" value={avgRow.avg_ev} display={avgRow.avg_ev != null ? avgRow.avg_ev.toFixed(1) : "—"} />
              <StatCell metric="slg" value={avgRow.slg} display={fmt(avgRow.slg)} />
              <StatCell metric="woba" value={avgRow.woba} display={fmt(avgRow.woba)} />
              <StatCell metric="k_pct" value={avgRow.k_pct} display={avgRow.k_pct != null ? `${avgRow.k_pct.toFixed(0)}%` : "—"} />
              <StatCell metric="obp" value={avgRow.obp} display={fmt(avgRow.obp)} />
            </View>
          ) : null}
        </View>
      </ScrollView>
    </View>
  );
}

// ── Collapsible Pitcher Pitch Mix section ────────────────────────────────────

const MIX_TYPE_W = 100;
const MIX_NUM_W = 34;
const MIX_PCT_W = 42;
const MIX_STAT_W = 50;

function PitchMixStatCell({ metric, value, display }: { metric: string; value?: number | null; display: string }) {
  return (
    <View style={[s.mixStatCell, { backgroundColor: heatBg(metric, value) }]}>
      <Text style={s.mixStatCellText}>{display}</Text>
    </View>
  );
}

function PitcherPitchMixSection({
  pitcherName,
  pitcherHand,
  mixVsLhb,
  mixVsRhb,
}: {
  pitcherName: string;
  pitcherHand: string;
  mixVsLhb: MlbPitchMixRow[];
  mixVsRhb: MlbPitchMixRow[];
}) {
  const [expanded, setExpanded] = useState(false);
  const [handTab, setHandTab] = useState<"lhb" | "rhb">("rhb");
  const pitcherHandLabel = (pitcherHand ?? "R").toUpperCase().startsWith("L") ? "LHP" : "RHP";
  const rows = handTab === "lhb" ? mixVsLhb : mixVsRhb;

  if (mixVsLhb.length === 0 && mixVsRhb.length === 0) return null;

  return (
    <View style={s.mixSection}>
      <Pressable style={s.mixToggleRow} onPress={() => setExpanded((e) => !e)}>
        <View style={s.mixToggleLeft}>
          <Text style={s.mixToggleChevron}>{expanded ? "▾" : "▸"}</Text>
          <Text style={s.mixToggleTitle}>Pitch Mix</Text>
          <Text style={s.mixToggleSub}>{pitcherName}</Text>
          <View style={s.pitcherHandBadge}>
            <Text style={s.pitcherHandText}>{pitcherHandLabel}</Text>
          </View>
        </View>
      </Pressable>

      {expanded ? (
        <View style={s.mixContent}>
          {/* vs LHB / vs RHB tabs */}
          <View style={s.mixTabRow}>
            <Pressable
              style={[s.mixTab, handTab === "lhb" ? s.mixTabActive : s.mixTabInactive]}
              onPress={() => setHandTab("lhb")}
            >
              <Text style={handTab === "lhb" ? s.mixTabTextActive : s.mixTabTextInactive}>vs LHB</Text>
            </Pressable>
            <Pressable
              style={[s.mixTab, handTab === "rhb" ? s.mixTabActive : s.mixTabInactive]}
              onPress={() => setHandTab("rhb")}
            >
              <Text style={handTab === "rhb" ? s.mixTabTextActive : s.mixTabTextInactive}>vs RHB</Text>
            </Pressable>
          </View>

          {/* Pitch mix table */}
          <ScrollView horizontal showsHorizontalScrollIndicator={false}>
            <View>
              {/* Header */}
              <View style={s.mixHeaderRow}>
                <View style={{ width: MIX_TYPE_W, paddingLeft: 4 }}><Text style={s.colHeader}>TYPE</Text></View>
                <View style={{ width: MIX_NUM_W, alignItems: "center" }}><Text style={s.colHeader}>#</Text></View>
                <View style={{ width: MIX_PCT_W, alignItems: "center" }}><Text style={s.colHeader}>%</Text></View>
                <View style={{ width: MIX_STAT_W, alignItems: "center" }}><Text style={s.colHeader}>BA</Text></View>
                <View style={{ width: MIX_STAT_W, alignItems: "center" }}><Text style={s.colHeader}>wOBA</Text></View>
                <View style={{ width: MIX_STAT_W, alignItems: "center" }}><Text style={s.colHeader}>SLG</Text></View>
                <View style={{ width: MIX_STAT_W, alignItems: "center" }}><Text style={s.colHeader}>ISO</Text></View>
                <View style={{ width: MIX_NUM_W, alignItems: "center" }}><Text style={s.colHeader}>HR</Text></View>
                <View style={{ width: MIX_STAT_W, alignItems: "center" }}><Text style={s.colHeader}>K%</Text></View>
                <View style={{ width: MIX_STAT_W, alignItems: "center" }}><Text style={s.colHeader}>WHIFF%</Text></View>
              </View>

              {/* Rows */}
              {rows.map((row, idx) => {
                const color = pitchColor(row.pitch_name);
                return (
                  <View key={`${row.pitch_name}-${idx}`} style={s.mixDataRow}>
                    <View style={{ width: MIX_TYPE_W, paddingLeft: 4, flexDirection: "row", alignItems: "center", gap: 6 }}>
                      <View style={[s.mixPitchDot, { backgroundColor: color }]} />
                      <Text style={s.mixPitchName} numberOfLines={1}>{row.pitch_name ?? "—"}</Text>
                    </View>
                    <View style={{ width: MIX_NUM_W, alignItems: "center", justifyContent: "center" }}>
                      <Text style={s.mixNumText}>{row.pitch_count ?? "—"}</Text>
                    </View>
                    <View style={{ width: MIX_PCT_W, alignItems: "center", justifyContent: "center" }}>
                      <Text style={s.mixNumText}>
                        {row.pitch_pct != null ? `${row.pitch_pct.toFixed(1)}%` : "—"}
                      </Text>
                    </View>
                    <PitchMixStatCell metric="avg" value={row.ba} display={fmt(row.ba)} />
                    <PitchMixStatCell metric="woba" value={row.woba} display={fmt(row.woba)} />
                    <PitchMixStatCell metric="slg" value={row.slg} display={fmt(row.slg)} />
                    <PitchMixStatCell metric="iso" value={row.iso} display={fmt(row.iso)} />
                    <View style={{ width: MIX_NUM_W, alignItems: "center", justifyContent: "center" }}>
                      <Text style={s.mixNumText}>{row.hr ?? 0}</Text>
                    </View>
                    <PitchMixStatCell metric="k_pct" value={row.k_pct} display={row.k_pct != null ? `${row.k_pct.toFixed(1)}%` : "—"} />
                    <View style={[s.mixStatCell, { backgroundColor: heatBg("k_pct", row.whiff_pct) }]}>
                      <Text style={s.mixStatCellText}>{row.whiff_pct != null ? `${row.whiff_pct.toFixed(1)}%` : "—"}</Text>
                    </View>
                  </View>
                );
              })}

              {rows.length === 0 ? (
                <Text style={[s.emptyText, { padding: 8 }]}>No pitch data for this split.</Text>
              ) : null}
            </View>
          </ScrollView>
        </View>
      ) : null}
    </View>
  );
}

// ── Reusable game content (pitcher panels + batter rows) ───────────────────

export function HrMatchupGameContent({
  gamePk,
  selectedKeys,
  onToggleSelect,
}: {
  gamePk: number;
  selectedKeys: Set<string>;
  onToggleSelect: (batterId: string) => void;
}) {
  const { colors } = useTheme();
  const [season, setSeason] = useState(2026);
  const { data, loading, error, refetch } = useMlbMatchupDetail(Number.isFinite(gamePk) ? gamePk : null, season);
  const { data: boData } = useMlbBattingOrder(Number.isFinite(gamePk) ? gamePk : null);

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

  if (loading) return <ActivityIndicator color="#93C5FD" />;

  if (error) {
    return (
      <Pressable onPress={refetch} style={[s.errorBox, { borderColor: colors.border.subtle }]}>
        <Text style={s.errorTitle}>Failed to load matchup.</Text>
        <Text style={s.errorText}>{error}</Text>
        <Text style={s.errorRetry}>Tap to retry</Text>
      </Pressable>
    );
  }

  if (!(data?.pitchers?.length)) {
    return (
      <View style={[s.panel, { borderColor: colors.border.subtle }]}>
        <Text style={s.emptyTitle}>No MLB model data for this game yet.</Text>
        <Text style={s.emptyText}>Data will populate once the model run completes.</Text>
      </View>
    );
  }

  return (
    <>
      {/* Season toggle */}
      <View style={s.seasonRow}>
        <Pressable
          style={[s.seasonBtn, season === 2025 ? s.seasonBtnActive : s.seasonBtnInactive]}
          onPress={() => setSeason(2025)}
        >
          <Text style={season === 2025 ? s.seasonBtnTextActive : s.seasonBtnTextInactive}>2025</Text>
        </Pressable>
        <Pressable
          style={[s.seasonBtn, season === 2026 ? s.seasonBtnActive : s.seasonBtnInactive]}
          onPress={() => setSeason(2026)}
        >
          <Text style={season === 2026 ? s.seasonBtnTextActive : s.seasonBtnTextInactive}>2026</Text>
        </Pressable>
      </View>

      {data.pitchers.map((pitcher) => {
        const batters = dedupeBatters(pitcher.batters ?? []).slice(0, 12);
        const pitcherHandRaw = (pitcher.pitcher_hand ?? "R").toUpperCase();
        const pitcherHandLabel = pitcherHandRaw.startsWith("L") ? "LHP" : "RHP";
        const mixVsRhb = pitcher.pitch_mix?.vs_rhb ?? [];
        const mixVsLhb = pitcher.pitch_mix?.vs_lhb ?? [];

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
            {/* Pitcher header */}
            <View style={s.pitcherHeader}>
              <Text style={s.pitcherLabel}>Opposing Pitcher:</Text>
              <Text style={s.pitcherName}>{pitcher.pitcher_name ?? "Pitcher"}</Text>
              <View style={s.pitcherHandBadge}>
                <Text style={s.pitcherHandText}>{pitcherHandLabel}</Text>
              </View>
            </View>

            <HandednessSection
              label="vs RHB"
              pitcherName={pitcher.pitcher_name ?? "Pitcher"}
              batters={rhBatters}
              pitcherMix={mixVsRhb}
              pitcherHand={pitcherHandRaw}
              weakSpotIds={weakSpotMap.get(pitcher.pitcher_id ?? -1) ?? new Set()}
              selectedKeys={selectedKeys}
              onToggleSelect={onToggleSelect}
            />

            <HandednessSection
              label="vs LHB"
              pitcherName={pitcher.pitcher_name ?? "Pitcher"}
              batters={lhBatters}
              pitcherMix={mixVsLhb}
              pitcherHand={pitcherHandRaw}
              weakSpotIds={weakSpotMap.get(pitcher.pitcher_id ?? -1) ?? new Set()}
              selectedKeys={selectedKeys}
              onToggleSelect={onToggleSelect}
            />

            {/* Collapsible pitcher pitch mix breakdown */}
            <PitcherPitchMixSection
              pitcherName={pitcher.pitcher_name ?? "Pitcher"}
              pitcherHand={pitcherHandRaw}
              mixVsLhb={mixVsLhb}
              mixVsRhb={mixVsRhb}
            />

            {!batters.length ? (
              <Text style={s.emptyText}>No hitter data for this pitcher.</Text>
            ) : null}
          </View>
        );
      })}
    </>
  );
}

// ── Main screen ─────────────────────────────────────────────────────────────

export function MlbHrMatchupScreen() {
  const { colors } = useTheme();
  const router = useRouter();
  const params = useLocalSearchParams<{ gamePk?: string; awayTeam?: string; homeTeam?: string }>();
  const gamePk = Number(params.gamePk);
  const [season, setSeason] = useState(2026);
  const { data, loading, error, refetch } = useMlbMatchupDetail(Number.isFinite(gamePk) ? gamePk : null, season);
  const { data: boData } = useMlbBattingOrder(Number.isFinite(gamePk) ? gamePk : null);
  const platform = getBuildPlatform();

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
  const awayLogo = getMlbTeamLogo(awayTeam);
  const homeLogo = getMlbTeamLogo(homeTeam);

  const slipItems = usePropBetslip((st) => st.items);
  const addToSlip = usePropBetslip((st) => st.add);
  const removeFromSlip = usePropBetslip((st) => st.remove);
  const clearSlip = usePropBetslip((st) => st.clear);

  const selectedKeys = useMemo(() => {
    const keys = new Set<string>();
    for (const item of slipItems) {
      if (item.sport === "mlb" && item.market === "MLB 1+ HR") {
        keys.add(String(item.player_id));
      }
    }
    return keys;
  }, [slipItems]);

  function toggleSelect(batterId: string) {
    let found: MlbBatterPick | null = null;
    for (const pitcher of data?.pitchers ?? []) {
      for (const b of pitcher.batters ?? []) {
        if (String(b.batter_id ?? b.batter_name ?? "") === batterId) {
          found = b;
          break;
        }
      }
      if (found) break;
    }
    if (!found) return;

    const slipId = `mlb-hr-${batterId}`;
    if (slipItems.some((i) => i.id === slipId)) {
      removeFromSlip(slipId);
    } else if (slipItems.length < 10) {
      addToSlip({
        id: slipId,
        player_id: Number(found.batter_id ?? 0),
        player: found.batter_name ?? "Batter",
        market: "MLB 1+ HR",
        side: "over",
        line: 0.5,
        odds: Number(found.hr_odds_best_price ?? 100),
        sport: "mlb",
        bookmaker: null,
        dk_outcome_code: found.dk_outcome_code ?? null,
        dk_event_id: found.dk_event_id ?? null,
        fd_market_id: found.fd_market_id ?? null,
        fd_selection_id: found.fd_selection_id ?? null,
      });
    }
  }

  const fdLink = useMemo(() => {
    const mlbItems = slipItems.filter((i) => i.sport === "mlb" && i.market === "MLB 1+ HR");
    if (mlbItems.length === 0) return null;
    return buildFanDuelParlay(
      mlbItems.map((i) => ({
        fd_market_id: i.fd_market_id ?? null,
        fd_selection_id: i.fd_selection_id ?? null,
      })),
      platform
    );
  }, [slipItems, platform]);

  const dkLink = useMemo(() => {
    const mlbItems = slipItems.filter((i) => i.sport === "mlb" && i.market === "MLB 1+ HR");
    if (mlbItems.length === 0) return null;
    return buildDraftKingsParlay(
      mlbItems.map((i) => ({
        dk_outcome_code: i.dk_outcome_code ?? null,
        dk_event_id: i.dk_event_id ?? null,
      })),
      platform
    );
  }, [slipItems, platform]);

  const mlbSlipItems = useMemo(
    () => slipItems.filter((i) => i.sport === "mlb" && i.market === "MLB 1+ HR"),
    [slipItems]
  );

  function openUrl(url?: string | null) {
    if (!url) return;
    if (platform === "desktop" && typeof globalThis.open === "function") {
      globalThis.open(url, "_blank");
      return;
    }
    Linking.openURL(url).catch(() => {});
  }

  return (
  <View style={s.screen}>
    <ScrollView style={s.scrollView} contentContainerStyle={s.content}>
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

      {/* Season toggle */}
      <View style={s.seasonRow}>
        <Pressable
          style={[s.seasonBtn, season === 2025 ? s.seasonBtnActive : s.seasonBtnInactive]}
          onPress={() => setSeason(2025)}
        >
          <Text style={season === 2025 ? s.seasonBtnTextActive : s.seasonBtnTextInactive}>2025</Text>
        </Pressable>
        <Pressable
          style={[s.seasonBtn, season === 2026 ? s.seasonBtnActive : s.seasonBtnInactive]}
          onPress={() => setSeason(2026)}
        >
          <Text style={season === 2026 ? s.seasonBtnTextActive : s.seasonBtnTextInactive}>2026</Text>
        </Pressable>
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
        const pitcherHandLabel = pitcherHandRaw.startsWith("L") ? "LHP" : "RHP";
        const mixVsRhb = pitcher.pitch_mix?.vs_rhb ?? [];
        const mixVsLhb = pitcher.pitch_mix?.vs_lhb ?? [];

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
            <View style={s.pitcherHeader}>
              <Text style={s.pitcherLabel}>Opposing Pitcher:</Text>
              <Text style={s.pitcherName}>{pitcher.pitcher_name ?? "Pitcher"}</Text>
              <View style={s.pitcherHandBadge}>
                <Text style={s.pitcherHandText}>{pitcherHandLabel}</Text>
              </View>
            </View>

            <HandednessSection
              label="vs RHB"
              pitcherName={pitcher.pitcher_name ?? "Pitcher"}
              batters={rhBatters}
              pitcherMix={mixVsRhb}
              pitcherHand={pitcherHandRaw}
              weakSpotIds={weakSpotMap.get(pitcher.pitcher_id ?? -1) ?? new Set()}
              selectedKeys={selectedKeys}
              onToggleSelect={toggleSelect}
            />

            <HandednessSection
              label="vs LHB"
              pitcherName={pitcher.pitcher_name ?? "Pitcher"}
              batters={lhBatters}
              pitcherMix={mixVsLhb}
              pitcherHand={pitcherHandRaw}
              weakSpotIds={weakSpotMap.get(pitcher.pitcher_id ?? -1) ?? new Set()}
              selectedKeys={selectedKeys}
              onToggleSelect={toggleSelect}
            />

            {/* Collapsible pitcher pitch mix breakdown */}
            <PitcherPitchMixSection
              pitcherName={pitcher.pitcher_name ?? "Pitcher"}
              pitcherHand={pitcherHandRaw}
              mixVsLhb={mixVsLhb}
              mixVsRhb={mixVsRhb}
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

    {/* ── Fixed bottom parlay bar ── */}
    {mlbSlipItems.length >= 1 ? (
      <View style={s.parlayBar}>
        <View style={s.parlayTopRow}>
          <Text style={s.parlayTitle}>
            {mlbSlipItems.length} batter{mlbSlipItems.length !== 1 ? "s" : ""} selected
          </Text>
          <Pressable onPress={clearSlip}>
            <Text style={s.parlayClear}>✕</Text>
          </Pressable>
        </View>
        <View style={s.parlayLegs}>
          {mlbSlipItems.map((item) => (
            <Text key={item.id} style={s.parlayLegText}>
              • {item.player} — 1+ HR {fmtOdds(item.odds)}
            </Text>
          ))}
        </View>
        <View style={s.parlayBtnRow}>
          <Pressable
            style={[s.parlayBtn, !dkLink ? s.parlayBtnDisabled : null]}
            disabled={!dkLink}
            onPress={() => openUrl(dkLink)}
          >
            <Text style={s.parlayBtnText}>
              {mlbSlipItems.length === 1 ? "Bet DraftKings" : `DK Parlay (${mlbSlipItems.length})`}
            </Text>
          </Pressable>
          <Pressable
            style={[s.parlayBtn, !fdLink ? s.parlayBtnDisabled : null]}
            disabled={!fdLink}
            onPress={() => openUrl(fdLink)}
          >
            <Text style={s.parlayBtnText}>
              {mlbSlipItems.length === 1 ? "Bet FanDuel" : `FD Parlay (${mlbSlipItems.length})`}
            </Text>
          </Pressable>
        </View>
        <Text style={s.parlayNote}>Parlay availability subject to sportsbook approval.</Text>
      </View>
    ) : null}
  </View>
  );
}

// ── Styles ──────────────────────────────────────────────────────────────────

const STAT_W = 52;
const PLAYER_W = 130;
const PA_W = 34;
const RANK_W = 22;
const HAB_W = 52;
const CHECK_W = 30;
const HR_W = 30;

const s = StyleSheet.create({
  screen: { flex: 1, backgroundColor: "#050A18" },
  scrollView: { flex: 1 },
  content: { padding: 12, gap: 10, paddingBottom: 40 },

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
  panel: { borderWidth: StyleSheet.hairlineWidth, borderRadius: 14, backgroundColor: "#0B1529", padding: 12, gap: 10 },

  // Pitcher header
  pitcherHeader: {
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
    paddingBottom: 6,
    borderBottomWidth: StyleSheet.hairlineWidth,
    borderBottomColor: "#1E293B",
  },
  pitcherLabel: { color: "#64748B", fontSize: 11, fontWeight: "600" },
  pitcherName: { color: "#E5E7EB", fontSize: 16, fontWeight: "800" },
  pitcherHandBadge: {
    borderWidth: 1,
    borderColor: "#6366F1",
    borderRadius: 4,
    paddingHorizontal: 6,
    paddingVertical: 2,
    backgroundColor: "rgba(99,102,241,0.15)",
  },
  pitcherHandText: { color: "#A5B4FC", fontSize: 10, fontWeight: "800" },

  // Handedness section
  handSection: { gap: 6, marginTop: 10 },
  handSectionTitle: { color: "#94A3B8", fontSize: 12, fontWeight: "700", marginBottom: 2 },

  // Pitch toggle pills
  pitchToggleWrap: { flexDirection: "row", alignItems: "center", marginBottom: 6 },
  pitchToggleRow: { flex: 1 },
  pitchToggleInner: { flexDirection: "row", gap: 6, alignItems: "center" },
  pitchPill: {
    borderWidth: 1,
    borderRadius: 4,
    paddingHorizontal: 8,
    paddingVertical: 4,
  },
  pitchPillInactive: {
    borderColor: "#334155",
    backgroundColor: "#0F172A",
  },
  pitchPillText: { color: "#94A3B8", fontSize: 10, fontWeight: "700" },
  pitchClearBtn: {
    borderWidth: 1,
    borderColor: "#334155",
    borderRadius: 999,
    paddingHorizontal: 10,
    paddingVertical: 4,
    marginLeft: 6,
  },
  pitchClearText: { color: "#64748B", fontSize: 10, fontWeight: "700" },

  // Table
  tableHeaderRow: {
    flexDirection: "row",
    borderBottomWidth: 1,
    borderBottomColor: "#334155",
    paddingBottom: 6,
    paddingTop: 2,
    alignItems: "center",
  },
  colHeader: { color: "#64748B", fontSize: 9, fontWeight: "800", textAlign: "center" },
  rankCol: { width: RANK_W, alignItems: "center", justifyContent: "center" },
  playerCol: { width: PLAYER_W, paddingLeft: 2, justifyContent: "center" },
  paCol: { width: PA_W, alignItems: "center", justifyContent: "center" },
  habCol: { width: HAB_W, alignItems: "center", justifyContent: "center" },
  statColHdr: { width: STAT_W, alignItems: "center", justifyContent: "center" },
  checkCol: { width: CHECK_W, alignItems: "center", justifyContent: "center" },
  hrCol: { width: HR_W, alignItems: "center", justifyContent: "center" },

  // Table row
  tableRow: {
    flexDirection: "row",
    borderBottomWidth: StyleSheet.hairlineWidth,
    borderBottomColor: "rgba(51,65,85,0.4)",
    minHeight: 38,
    alignItems: "center",
  },
  tableRowWeak: {
    borderLeftWidth: 2,
    borderLeftColor: "#10B981",
  },
  tableRowSelected: {
    backgroundColor: "rgba(16,185,129,0.08)",
  },

  // Checkbox
  checkOn: { color: "#10B981", fontSize: 16, fontWeight: "800" },
  checkOff: { color: "#475569", fontSize: 16 },

  // Player info in row
  playerRank: { color: "#475569", fontSize: 10, fontWeight: "700", textAlign: "center" },
  playerNameWrap: { flex: 1, gap: 1 },
  playerName: { color: "#E5E7EB", fontSize: 11, fontWeight: "700" },
  playerMeta: { color: "#64748B", fontSize: 9, fontWeight: "600" },

  // PA / H-AB cells
  paCellText: { color: "#94A3B8", fontSize: 10, fontWeight: "700", textAlign: "center" },

  // Stat cell with heat-map background
  statCell: {
    width: STAT_W,
    alignItems: "center",
    justifyContent: "center",
    paddingVertical: 7,
    borderLeftWidth: StyleSheet.hairlineWidth,
    borderLeftColor: "rgba(51,65,85,0.3)",
  },
  statCellText: { color: "#E2E8F0", fontSize: 10, fontWeight: "700" },

  // Averages row
  avgRow: {
    flexDirection: "row",
    borderTopWidth: 1,
    borderTopColor: "#334155",
    backgroundColor: "rgba(15,23,42,0.6)",
    minHeight: 34,
    alignItems: "center",
  },
  avgLabel: { color: "#94A3B8", fontSize: 11, fontWeight: "800" },
  avgCellText: { color: "#94A3B8", fontSize: 10, fontWeight: "800", textAlign: "center" },

  // Season toggle
  seasonRow: {
    flexDirection: "row",
    gap: 0,
    marginBottom: 8,
    alignSelf: "flex-end",
  },
  seasonBtn: {
    paddingHorizontal: 14,
    paddingVertical: 6,
    borderWidth: 1,
  },
  seasonBtnActive: {
    backgroundColor: "rgba(99,102,241,0.25)",
    borderColor: "#6366F1",
    borderRadius: 4,
  },
  seasonBtnInactive: {
    backgroundColor: "transparent",
    borderColor: "#334155",
    borderRadius: 4,
  },
  seasonBtnTextActive: { color: "#A5B4FC", fontSize: 12, fontWeight: "800" },
  seasonBtnTextInactive: { color: "#64748B", fontSize: 12, fontWeight: "700" },

  // Pitcher Pitch Mix collapsible section
  mixSection: {
    marginTop: 10,
    borderTopWidth: 1,
    borderTopColor: "#1E293B",
    paddingTop: 8,
  },
  mixToggleRow: {
    flexDirection: "row",
    alignItems: "center",
    paddingVertical: 6,
  },
  mixToggleLeft: {
    flexDirection: "row",
    alignItems: "center",
    gap: 6,
    flex: 1,
  },
  mixToggleChevron: { color: "#64748B", fontSize: 12, width: 14 },
  mixToggleTitle: { color: "#E5E7EB", fontSize: 13, fontWeight: "800" },
  mixToggleSub: { color: "#94A3B8", fontSize: 12, fontWeight: "600" },
  mixContent: { gap: 8, marginTop: 4 },
  mixTabRow: { flexDirection: "row", gap: 0 },
  mixTab: {
    paddingHorizontal: 14,
    paddingVertical: 6,
    borderRadius: 4,
  },
  mixTabActive: { backgroundColor: "rgba(99,102,241,0.25)" },
  mixTabInactive: { backgroundColor: "transparent" },
  mixTabTextActive: { color: "#A5B4FC", fontSize: 11, fontWeight: "800" },
  mixTabTextInactive: { color: "#64748B", fontSize: 11, fontWeight: "700" },
  mixHeaderRow: {
    flexDirection: "row",
    borderBottomWidth: 1,
    borderBottomColor: "#334155",
    paddingBottom: 6,
    paddingTop: 2,
    alignItems: "center",
  },
  mixDataRow: {
    flexDirection: "row",
    borderBottomWidth: StyleSheet.hairlineWidth,
    borderBottomColor: "rgba(51,65,85,0.4)",
    minHeight: 34,
    alignItems: "center",
  },
  mixPitchDot: { width: 8, height: 8, borderRadius: 4 },
  mixPitchName: { color: "#E5E7EB", fontSize: 11, fontWeight: "700", flex: 1 },
  mixNumText: { color: "#94A3B8", fontSize: 10, fontWeight: "700" },
  mixStatCell: {
    width: MIX_STAT_W,
    alignItems: "center",
    justifyContent: "center",
    paddingVertical: 6,
    borderLeftWidth: StyleSheet.hairlineWidth,
    borderLeftColor: "rgba(51,65,85,0.3)",
  },
  mixStatCellText: { color: "#E2E8F0", fontSize: 10, fontWeight: "700" },

  // Error / empty
  errorBox: { borderWidth: StyleSheet.hairlineWidth, borderRadius: 12, backgroundColor: "#1F2937", padding: 12 },
  errorTitle: { color: "#FCA5A5", fontWeight: "700" },
  errorText: { color: "#FECACA", marginTop: 4, fontSize: 12 },
  errorRetry: { color: "#E5E7EB", marginTop: 8, fontSize: 12 },
  emptyTitle: { color: "#E5E7EB", fontWeight: "700" },
  emptyText: { color: "#A7C0E8", marginTop: 6, fontSize: 12 },

  // Parlay bar
  parlayBar: {
    borderTopWidth: 1,
    borderTopColor: "#10B981",
    backgroundColor: "rgba(2,6,23,0.98)",
    padding: 12,
    gap: 8,
  },
  parlayTopRow: { flexDirection: "row", justifyContent: "space-between", alignItems: "center" },
  parlayTitle: { color: "#E2E8F0", fontSize: 14, fontWeight: "800" },
  parlayClear: { color: "#94A3B8", fontSize: 16, fontWeight: "900", paddingHorizontal: 4 },
  parlayLegs: { gap: 2 },
  parlayLegText: { color: "#C7D2FE", fontSize: 11 },
  parlayBtnRow: { flexDirection: "row", gap: 8 },
  parlayBtn: {
    flex: 1,
    borderWidth: StyleSheet.hairlineWidth,
    borderColor: "#10B981",
    borderRadius: 10,
    backgroundColor: "rgba(16,185,129,0.12)",
    paddingVertical: 10,
    alignItems: "center",
  },
  parlayBtnDisabled: { opacity: 0.4, borderColor: "#334155" },
  parlayBtnText: { color: "#A7F3D0", fontSize: 13, fontWeight: "800" },
  parlayNote: { color: "#64748B", fontSize: 9 },
});

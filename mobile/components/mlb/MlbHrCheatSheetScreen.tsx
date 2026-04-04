import { useCallback, useMemo, useState } from "react";
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

import {
  useMlbHrCheatSheet,
  useMlbCheatSheetBatterDetail,
  useCheatSheetBatchPreFetch,
  type CheatSheetBatter,
  type CheatSheetBatterDetail,
} from "@/hooks/mlb/useMlbMatchups";
import { useTheme } from "@/store/useTheme";
import { usePropBetslip, type PropSlipItem } from "@/store/usePropBetslip";
import { buildFanDuelParlay, getBuildPlatform } from "@/utils/parlayBuilder";
import { getMlbTeamLogo } from "@/utils/mlbLogos";
import { formatET } from "@/lib/time/formatET";

// ── Helpers ────────────────────────────────────────────────────────────────

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

type GradeKey = "A+" | "A" | "B" | "C" | "D";

const GRADE_CONFIG: Record<GradeKey, { label: string; border: string; bg: string; text: string }> = {
  "A+": { label: "A+  IDEAL", border: "#10B981", bg: "rgba(16,185,129,0.12)", text: "#A7F3D0" },
  A: { label: "A  FAVORABLE", border: "#22D3EE", bg: "rgba(34,211,238,0.12)", text: "#CFFAFE" },
  B: { label: "B  AVERAGE", border: "#F59E0B", bg: "rgba(245,158,11,0.12)", text: "#FDE68A" },
  C: { label: "C  BELOW AVG", border: "#F97316", bg: "rgba(249,115,22,0.12)", text: "#FED7AA" },
  D: { label: "D  AVOID", border: "#EF4444", bg: "rgba(239,68,68,0.12)", text: "#FECACA" },
};

const VISIBLE_GRADES: GradeKey[] = ["A+", "A", "B", "C"];

function greenTone(metric: string, value?: number | null) {
  if (value == null || !Number.isFinite(value)) return null;
  switch (metric) {
    case "iso": if (value > 0.2) return st.cellGreen; break;
    case "slg": if (value > 0.48) return st.cellGreen; break;
    case "barrel_pct": if (value > 12) return st.cellGreen; break;
    case "hh_pct": if (value > 38) return st.cellGreen; break;
    case "ev": if (value > 90) return st.cellGreen; break;
    case "avg": if (value > 0.28) return st.cellGreen; break;
    case "obp": if (value > 0.35) return st.cellGreen; break;
    case "woba": if (value > 0.35) return st.cellGreen; break;
  }
  return null;
}

function formatTrajectory(traj?: string | null): string {
  if (!traj) return "—";
  return traj.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
}

function formatResult(result?: string | null): string {
  if (!result) return "—";
  return result.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
}

// ── Mini bar component ────────────────────────────────────────────────────

function MiniBar({ label, value, max = 100, color = "#10B981" }: { label: string; value?: number | null; max?: number; color?: string }) {
  const pct = value != null && Number.isFinite(value) ? Math.min(value / max, 1) : 0;
  return (
    <View style={st.miniBarWrap}>
      <Text style={st.miniBarLabel}>{label}</Text>
      <View style={st.miniBarTrack}>
        <View style={[st.miniBarFill, { width: `${Math.round(pct * 100)}%`, backgroundColor: color }]} />
      </View>
      <Text style={st.miniBarValue}>{value != null && Number.isFinite(value) ? `${value.toFixed(1)}%` : "—"}</Text>
    </View>
  );
}

// ── Group batters by game within a grade ───────────────────────────────────

type GameGroup = {
  game_pk: number;
  home_team: string;
  away_team: string;
  start_time_utc: string | null;
  venue_name: string | null;
  batters: CheatSheetBatter[];
};

function groupByGame(batters: CheatSheetBatter[]): GameGroup[] {
  const map = new Map<number, GameGroup>();
  for (const b of batters) {
    const gpk = b.game_pk ?? 0;
    let group = map.get(gpk);
    if (!group) {
      group = {
        game_pk: gpk,
        home_team: b.home_team ?? "Home",
        away_team: b.away_team ?? "Away",
        start_time_utc: b.start_time_utc ?? null,
        venue_name: b.venue_name ?? null,
        batters: [],
      };
      map.set(gpk, group);
    }
    group.batters.push(b);
  }
  // Sort by start time ascending
  return Array.from(map.values()).sort(
    (a, b) => (a.start_time_utc ?? "9999").localeCompare(b.start_time_utc ?? "9999")
  );
}

// ── Batter row ─────────────────────────────────────────────────────────────

function ExpandedBatterDetail({
  batter,
  detail,
  loading,
}: {
  batter: CheatSheetBatter;
  detail: CheatSheetBatterDetail | null;
  loading: boolean;
}) {
  const bvp = detail?.bvp_career;
  const hasBvp = bvp != null && (bvp.pa ?? 0) > 0;
  const bb = detail?.bvp_batted_ball;
  const profile = bb?.profile;
  const fullLog = bb?.log ?? [];
  const log = fullLog.slice(0, 20);
  const hasProfile = profile != null && (profile.total_batted ?? 0) > 0;
  const splits = detail?.pitcher_splits;
  const batSide = (batter.bat_side ?? "R").toUpperCase();
  const splitKey = batSide === "L" ? "vsLHB" : "vsRHB";
  const vsSplit = splits?.[splitKey];

  return (
    <View style={st.expandedArea}>
      {/* Quick stats row */}
      <View style={st.expandedRow}>
        <View style={st.expandedStat}>
          <Text style={st.expandedLabel}>SCORE</Text>
          <Text style={st.expandedValue}>{batter.score?.toFixed(1) ?? "—"}</Text>
        </View>
        <View style={st.expandedStat}>
          <Text style={st.expandedLabel}>L15 EV</Text>
          <Text style={[st.expandedValue, greenTone("ev", batter.l15_ev)]}>{batter.l15_ev?.toFixed(1) ?? "—"}</Text>
        </View>
        <View style={st.expandedStat}>
          <Text style={st.expandedLabel}>SZN EV</Text>
          <Text style={[st.expandedValue, greenTone("ev", batter.season_ev)]}>{batter.season_ev?.toFixed(1) ?? "—"}</Text>
        </View>
        <View style={st.expandedStat}>
          <Text style={st.expandedLabel}>ODDS</Text>
          <Text style={st.expandedValue}>{fmtOdds(batter.hr_odds_best_price)}</Text>
        </View>
        <View style={st.expandedStat}>
          <Text style={st.expandedLabel}>BOOK</Text>
          <Text style={st.expandedValue}>{batter.hr_odds_best_book ?? "—"}</Text>
        </View>
      </View>

      {loading ? (
        <ActivityIndicator color="#93C5FD" size="small" style={{ marginVertical: 8 }} />
      ) : null}

      {/* Career vs Pitcher */}
      {!loading && detail ? (
        <>
          <Text style={st.sectionTitle}>Career vs Pitcher</Text>
          {hasBvp ? (
            <View style={st.bvpWrap}>
              <Text style={st.bvpSummaryText}>
                {bvp!.hits ?? 0}-{bvp!.pa ?? 0} · {bvp!.hr ?? 0} HR
              </Text>
              <View style={st.bvpTable}>
                <View style={st.bvpHeaderRow}>
                  <Text style={st.bvpHeaderCell}>AVG</Text>
                  <Text style={st.bvpHeaderCell}>ISO</Text>
                  <Text style={st.bvpHeaderCell}>SLG</Text>
                  <Text style={st.bvpHeaderCell}>OBP</Text>
                  <Text style={st.bvpHeaderCell}>K%</Text>
                  <Text style={st.bvpHeaderCell}>BB%</Text>
                </View>
                <View style={st.bvpValueRow}>
                  <Text style={[st.bvpValueCell, greenTone("avg", bvp!.avg)]}>{fmt(bvp!.avg)}</Text>
                  <Text style={[st.bvpValueCell, greenTone("iso", bvp!.iso)]}>{fmt(bvp!.iso)}</Text>
                  <Text style={[st.bvpValueCell, greenTone("slg", bvp!.slg)]}>{fmt(bvp!.slg)}</Text>
                  <Text style={[st.bvpValueCell, greenTone("obp", bvp!.obp)]}>{fmt(bvp!.obp)}</Text>
                  <Text style={st.bvpValueCell}>{fmtPct(bvp!.k_pct)}</Text>
                  <Text style={st.bvpValueCell}>{fmtPct(bvp!.bb_pct)}</Text>
                </View>
              </View>
            </View>
          ) : (
            <Text style={st.bvpEmptyText}>No career data vs this pitcher</Text>
          )}

          {/* Batted Ball History */}
          {(hasProfile || log.length > 0) ? (
            <View style={st.bbWrap}>
              <Text style={st.sectionTitle}>
                Batted Ball History{fullLog.length > 20 ? ` (last 20 of ${fullLog.length})` : fullLog.length > 0 ? ` (${fullLog.length} batted)` : profile?.total_batted ? ` (${profile.total_batted} batted)` : ""}
              </Text>
              {hasProfile ? (
                <View style={st.bbBarsContainer}>
                  <View style={st.bbBarsRow}>
                    <MiniBar label="Barrel%" value={profile!.barrel_pct} max={25} color="#10B981" />
                    <MiniBar label="HH%" value={profile!.hh_pct} max={60} color="#10B981" />
                    <MiniBar label="FB%" value={profile!.fb_pct} max={60} color="#60A5FA" />
                    <MiniBar label="GB%" value={profile!.gb_pct} max={60} color="#F59E0B" />
                    <MiniBar label="LD%" value={profile!.ld_pct} max={40} color="#34D399" />
                    <MiniBar label="PU%" value={profile!.pu_pct} max={30} color="#94A3B8" />
                    <MiniBar label="HR/FB%" value={profile!.hr_fb_pct} max={30} color="#F87171" />
                  </View>
                  <View style={st.bbBarsRow}>
                    <MiniBar label="Pull%" value={profile!.pull_pct} max={60} color="#A78BFA" />
                    <MiniBar label="Str%" value={profile!.str_pct} max={50} color="#A78BFA" />
                    <MiniBar label="Oppo%" value={profile!.oppo_pct} max={40} color="#A78BFA" />
                  </View>
                </View>
              ) : null}
              {log.length > 0 ? (
                <ScrollView horizontal showsHorizontalScrollIndicator={false}>
                  <View>
                    <View style={st.logHeaderRow}>
                      <Text style={[st.logHeaderCell, st.logDateCol]}>DATE</Text>
                      <Text style={[st.logHeaderCell, st.logPitchCol]}>PITCH</Text>
                      <Text style={st.logHeaderCell}>EV</Text>
                      <Text style={st.logHeaderCell}>ANGLE</Text>
                      <Text style={st.logHeaderCell}>DIST</Text>
                      <Text style={[st.logHeaderCell, st.logTrajCol]}>TRAJ</Text>
                      <Text style={[st.logHeaderCell, st.logResultCol]}>RESULT</Text>
                      <Text style={st.logHeaderCell}>PARKS</Text>
                    </View>
                    <ScrollView
                      style={st.logScrollContainer}
                      nestedScrollEnabled
                      showsVerticalScrollIndicator
                      indicatorStyle="white"
                    >
                      {log.map((row, idx) => {
                        const ev = row.ev;
                        const evStyle = ev != null && ev >= 95 ? st.cellGreen : null;
                        const angleVal = row.angle;
                        const angleLaunch = angleVal != null && angleVal >= 10 && angleVal <= 30;
                        const isHr = (row.result ?? "").toLowerCase() === "home_run";
                        return (
                          <View key={`log-${idx}`} style={st.logRow}>
                            <Text style={[st.logCell, st.logDateCol]}>{row.date ?? "—"}</Text>
                            <Text style={[st.logCell, st.logPitchCol]}>{row.pitch ?? "—"}</Text>
                            <Text style={[st.logCell, evStyle]}>{ev != null ? ev.toFixed(1) : "—"}</Text>
                            <Text style={[st.logCell, angleLaunch ? st.cellGreen : null]}>{angleVal != null ? angleVal.toFixed(1) : "—"}</Text>
                            <Text style={st.logCell}>{row.dist != null ? row.dist.toFixed(0) : "—"}</Text>
                            <Text style={[st.logCell, st.logTrajCol]}>{formatTrajectory(row.trajectory)}</Text>
                            <Text style={[st.logCell, st.logResultCol, isHr ? st.cellGreen : null]}>{formatResult(row.result)}</Text>
                            <Text style={[st.logCell, (row.hr_parks ?? 0) >= 20 ? st.cellGreen : null]}>{row.hr_parks != null ? `${row.hr_parks}/30` : "—"}</Text>
                          </View>
                        );
                      })}
                    </ScrollView>
                    {log.length > 10 ? (
                      <Text style={st.logScrollHint}>↓ Scroll for more</Text>
                    ) : null}
                  </View>
                </ScrollView>
              ) : null}
            </View>
          ) : null}

          {/* Pitcher Stats vs Handedness */}
          {vsSplit ? (
            <View style={st.pitcherSplitWrap}>
              <Text style={st.sectionTitle}>
                {batter.pitcher_name ?? "Pitcher"} vs {batSide === "L" ? "LHB" : "RHB"}
              </Text>
              <View style={st.bvpTable}>
                <View style={st.bvpHeaderRow}>
                  <Text style={st.bvpHeaderCell}>HR/9</Text>
                  <Text style={st.bvpHeaderCell}>BRL%</Text>
                  <Text style={st.bvpHeaderCell}>HH%</Text>
                  <Text style={st.bvpHeaderCell}>FB%</Text>
                  <Text style={st.bvpHeaderCell}>HR/FB%</Text>
                  <Text style={st.bvpHeaderCell}>WHIP</Text>
                  <Text style={st.bvpHeaderCell}>wOBA</Text>
                </View>
                <View style={st.bvpValueRow}>
                  <Text style={st.bvpValueCell}>{vsSplit.hr_per_9?.toFixed(2) ?? "—"}</Text>
                  <Text style={st.bvpValueCell}>{fmtPct(vsSplit.barrel_pct)}</Text>
                  <Text style={st.bvpValueCell}>{fmtPct(vsSplit.hard_hit_pct)}</Text>
                  <Text style={st.bvpValueCell}>{fmtPct(vsSplit.fb_pct)}</Text>
                  <Text style={st.bvpValueCell}>{fmtPct(vsSplit.hr_fb_pct)}</Text>
                  <Text style={st.bvpValueCell}>{vsSplit.whip?.toFixed(2) ?? "—"}</Text>
                  <Text style={[st.bvpValueCell, greenTone("woba", vsSplit.woba)]}>{fmt(vsSplit.woba)}</Text>
                </View>
              </View>
            </View>
          ) : null}
        </>
      ) : null}

    </View>
  );
}

function CheatSheetBatterCard({
  batter,
  selected,
  onToggleSelect,
  expanded,
  onToggleExpand,
}: {
  batter: CheatSheetBatter;
  selected: boolean;
  onToggleSelect: () => void;
  expanded: boolean;
  onToggleExpand: () => void;
}) {
  const hasFd = !!(batter.fd_market_id && batter.fd_selection_id);
  const config = GRADE_CONFIG[(batter.grade === "IDEAL" ? "A+" : batter.grade === "FAVORABLE" ? "A" : batter.grade === "AVOID" ? "D" : batter.grade === "AVERAGE" ? "B" : "C") as GradeKey];
  const gradeLabel = batter.grade === "IDEAL" ? "Ideal Target" : batter.grade === "FAVORABLE" ? "Favorable" : batter.grade === "AVOID" ? "Avoid" : "Average";

  // Lazy-load detail data only when expanded
  const { data: detail, loading: detailLoading } = useMlbCheatSheetBatterDetail(
    expanded ? batter.batter_id : null,
    expanded ? batter.pitcher_id : null,
    expanded ? batter.game_pk : null,
    expanded ? batter.bat_side : null,
  );

  return (
    <View style={[st.card, selected ? st.cardSelected : null, batter.is_weak_spot ? st.cardWeakSpot : null]}>
      {/* Header: name + hand + odds + grade pill */}
      <Pressable onPress={onToggleExpand} style={st.cardHeader}>
        <View style={st.cardNameRow}>
          <Text style={st.cardName} numberOfLines={1}>
            {batter.is_weak_spot ? "🎯 " : ""}{batter.batter_name ?? "—"}
          </Text>
          <Text style={st.cardHand}>{handLabel(batter.bat_side)}</Text>
          <Text style={st.cardOdds}>{fmtOdds(batter.hr_odds_best_price)}</Text>
        </View>
        <View style={st.cardSubRow}>
          <View style={[st.gradePill, { backgroundColor: config.bg, borderColor: config.border }]}>
            <Text style={[st.gradePillText, { color: config.text }]}>{gradeLabel}</Text>
          </View>
          {batter.hr_odds_best_book ? (
            <Text style={st.cardBook}>{batter.hr_odds_best_book}</Text>
          ) : null}
          {batter.pitcher_name ? (
            <Text style={st.cardVs}>vs {batter.pitcher_name}</Text>
          ) : null}
          <Text style={st.cardChevron}>{expanded ? "▾" : "▸"}</Text>
        </View>
      </Pressable>

      {/* Big stat numbers */}
      <View style={st.cardStatsRow}>
        <View style={st.cardBigStat}>
          <Text style={[st.cardBigNum, greenTone("iso", batter.iso)]}>{fmt(batter.iso)}</Text>
          <Text style={st.cardBigLabel}>ISO</Text>
        </View>
        <View style={st.cardBigStat}>
          <Text style={[st.cardBigNum, greenTone("slg", batter.slg)]}>{fmt(batter.slg)}</Text>
          <Text style={st.cardBigLabel}>SLG</Text>
        </View>
        <View style={st.cardBigStat}>
          <Text style={[st.cardBigNum, greenTone("ev", batter.l15_ev)]}>{batter.l15_ev?.toFixed(1) ?? "—"}</Text>
          <Text style={st.cardBigLabel}>L15 EV</Text>
        </View>
        <View style={st.cardBigStat}>
          <Text style={st.cardBigNum}>{batter.score?.toFixed(1) ?? "—"}</Text>
          <Text style={st.cardBigLabel}>SCORE</Text>
        </View>
      </View>

      {/* Secondary stats row */}
      <View style={st.cardSecondaryRow}>
        <View style={st.cardSmStat}>
          <Text style={[st.cardSmNum, greenTone("barrel_pct", batter.l15_barrel_pct)]}>{fmtPct(batter.l15_barrel_pct)}</Text>
          <Text style={st.cardSmLabel}>Barrel%</Text>
        </View>
        <View style={st.cardSmStat}>
          <Text style={[st.cardSmNum, greenTone("hh_pct", batter.l15_hard_hit_pct)]}>{fmtPct(batter.l15_hard_hit_pct)}</Text>
          <Text style={st.cardSmLabel}>HH%</Text>
        </View>
        <View style={st.cardSmStat}>
          <Text style={st.cardSmNum}>{fmtPct(batter.hr_fb_pct)}</Text>
          <Text style={st.cardSmLabel}>HR/FB%</Text>
        </View>
        <View style={st.cardSmStat}>
          <Text style={[st.cardSmNum, greenTone("ev", batter.season_ev)]}>{batter.season_ev?.toFixed(1) ?? "—"}</Text>
          <Text style={st.cardSmLabel}>SZN EV</Text>
        </View>
      </View>

      {/* Select / parlay button */}
      <Pressable
        style={[st.cardSelectBtn, selected ? st.cardSelectBtnActive : null, !hasFd ? st.cardSelectBtnDisabled : null]}
        onPress={hasFd ? onToggleSelect : undefined}
      >
        <Text style={[st.cardSelectBtnText, selected ? st.cardSelectBtnTextActive : null]}>
          {selected ? "✓ Selected" : "+ Add to Parlay"}
        </Text>
      </Pressable>

      {/* Expanded detail - lazy loaded */}
      {expanded ? (
        <ExpandedBatterDetail batter={batter} detail={detail} loading={detailLoading} />
      ) : null}
    </View>
  );
}

// ── Game header within a grade section ─────────────────────────────────────

function GameHeader({ group }: { group: GameGroup }) {
  const awayLogo = getMlbTeamLogo(group.away_team) ?? undefined;
  const homeLogo = getMlbTeamLogo(group.home_team) ?? undefined;

  return (
    <View style={st.gameHeader}>
      <View style={st.gameHeaderTeams}>
        {awayLogo ? <Image source={{ uri: awayLogo }} style={st.gameHeaderLogo} /> : null}
        <Text style={st.gameHeaderTeamText}>{group.away_team}</Text>
        <Text style={st.gameHeaderAt}>@</Text>
        {homeLogo ? <Image source={{ uri: homeLogo }} style={st.gameHeaderLogo} /> : null}
        <Text style={st.gameHeaderTeamText}>{group.home_team}</Text>
      </View>
      <Text style={st.gameHeaderTime}>
        {formatET(group.start_time_utc)} ET
        {group.venue_name ? ` · ${group.venue_name}` : ""}
      </Text>
    </View>
  );
}

// ── Grade section (collapsible) ────────────────────────────────────────────

function GradeSection({
  gradeKey,
  batters,
  selectedKeys,
  onToggleSelect,
  expandedIds,
  onToggleExpand,
}: {
  gradeKey: GradeKey;
  batters: CheatSheetBatter[];
  selectedKeys: Set<string>;
  onToggleSelect: (batterId: string) => void;
  expandedIds: Set<string>;
  onToggleExpand: (batterId: string) => void;
}) {
  const [collapsed, setCollapsed] = useState(false);
  const config = GRADE_CONFIG[gradeKey];
  const gameGroups = useMemo(() => groupByGame(batters), [batters]);

  if (batters.length === 0) return null;

  return (
    <View style={[st.gradeSection, { borderColor: config.border }]}>
      <Pressable style={st.gradeSectionHeader} onPress={() => setCollapsed((c) => !c)}>
        <View style={[st.gradeBadge, { backgroundColor: config.bg, borderColor: config.border }]}>
          <Text style={[st.gradeBadgeText, { color: config.text }]}>{gradeKey}</Text>
        </View>
        <Text style={[st.gradeSectionTitle, { color: config.text }]}>{config.label}</Text>
        <Text style={st.gradeSectionCount}>{batters.length}</Text>
        <Text style={st.gradeSectionChevron}>{collapsed ? "▸" : "▾"}</Text>
      </Pressable>

      {!collapsed ? (
        <View style={st.gradeSectionBody}>
          {gameGroups.map((group) => (
            <View key={group.game_pk}>
              <GameHeader group={group} />
              {group.batters.map((batter) => {
                const batterId = String(batter.batter_id ?? batter.batter_name ?? "");
                return (
                  <CheatSheetBatterCard
                    key={`${group.game_pk}-${batterId}`}
                    batter={batter}
                    selected={selectedKeys.has(batterId)}
                    onToggleSelect={() => onToggleSelect(batterId)}
                    expanded={expandedIds.has(`${group.game_pk}-${batterId}`)}
                    onToggleExpand={() => onToggleExpand(`${group.game_pk}-${batterId}`)}
                  />
                );
              })}
            </View>
          ))}
        </View>
      ) : null}
    </View>
  );
}

// ── Main cheat sheet screen ────────────────────────────────────────────────

export function MlbHrCheatSheetScreen() {
  const { colors } = useTheme();
  const { data, loading, error, refetch } = useMlbHrCheatSheet();
  const platform = getBuildPlatform();

  // Global betslip
  const slipItems = usePropBetslip((s) => s.items);
  const addToSlip = usePropBetslip((s) => s.add);
  const removeFromSlip = usePropBetslip((s) => s.remove);
  const clearSlip = usePropBetslip((s) => s.clear);

  const selectedKeys = useMemo(() => {
    const keys = new Set<string>();
    for (const item of slipItems) {
      if (item.sport === "mlb" && item.market === "MLB 1+ HR") {
        keys.add(String(item.player_id));
      }
    }
    return keys;
  }, [slipItems]);

  const [expandedIds, setExpandedIds] = useState<Set<string>>(new Set());

  const toggleExpand = useCallback((id: string) => {
    setExpandedIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }, []);

  // Find batter across all grades for toggle
  const allBatters = useMemo(() => {
    if (!data?.grades) return [];
    return [
      ...(data.grades["A+"] ?? []),
      ...(data.grades.A ?? []),
      ...(data.grades.B ?? []),
      ...(data.grades.C ?? []),
      ...(data.grades.D ?? []),
    ];
  }, [data]);

  // Pre-warm server cache for all batter details in background
  useCheatSheetBatchPreFetch(allBatters.length > 0 ? allBatters : null);

  const toggleSelect = useCallback(
    (batterId: string) => {
      const found = allBatters.find(
        (b) => String(b.batter_id ?? b.batter_name ?? "") === batterId
      );
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
          fd_market_id: found.fd_market_id ?? null,
          fd_selection_id: found.fd_selection_id ?? null,
        });
      }
    },
    [allBatters, slipItems, addToSlip, removeFromSlip]
  );

  const mlbSlipItems = useMemo(
    () => slipItems.filter((i) => i.sport === "mlb" && i.market === "MLB 1+ HR"),
    [slipItems]
  );

  const fdLink = useMemo(() => {
    if (mlbSlipItems.length === 0) return null;
    return buildFanDuelParlay(
      mlbSlipItems.map((i) => ({
        fd_market_id: i.fd_market_id ?? null,
        fd_selection_id: i.fd_selection_id ?? null,
      })),
      platform
    );
  }, [mlbSlipItems, platform]);

  function openUrl(url?: string | null) {
    if (!url) return;
    if (platform === "desktop" && typeof globalThis.open === "function") {
      globalThis.open(url, "_blank");
      return;
    }
    Linking.openURL(url).catch(() => {});
  }

  return (
    <View style={st.screen}>
      <ScrollView style={st.scrollView} contentContainerStyle={st.content}>
        <View style={[st.hero, { borderColor: colors.border.subtle }]}>
          <Text style={st.eyebrow}>MLB HR CHEAT SHEET</Text>
          <Text style={st.h1}>Home Run Picks by Grade</Text>
          <Text style={st.sub}>
            All batters across today&apos;s games organized by model grade. Tap to expand, select to build parlays.
          </Text>
          {data?.grade_counts ? (
            <View style={st.countsRow}>
              {VISIBLE_GRADES.map((g) => {
                const cfg = GRADE_CONFIG[g];
                const count = data.grade_counts[g] ?? 0;
                return (
                  <View key={g} style={[st.countPill, { borderColor: cfg.border }]}>
                    <Text style={[st.countPillText, { color: cfg.text }]}>
                      {g}: {count}
                    </Text>
                  </View>
                );
              })}
            </View>
          ) : null}
        </View>

        {loading ? <ActivityIndicator color="#93C5FD" /> : null}

        {error ? (
          <Pressable onPress={refetch} style={[st.errorBox, { borderColor: colors.border.subtle }]}>
            <Text style={st.errorTitle}>Failed to load cheat sheet.</Text>
            <Text style={st.errorText}>{error}</Text>
            <Text style={st.errorRetry}>Tap to retry</Text>
          </Pressable>
        ) : null}

        {data?.grades
          ? VISIBLE_GRADES.map((gradeKey) => (
              <GradeSection
                key={gradeKey}
                gradeKey={gradeKey}
                batters={data.grades[gradeKey] ?? []}
                selectedKeys={selectedKeys}
                onToggleSelect={toggleSelect}
                expandedIds={expandedIds}
                onToggleExpand={toggleExpand}
              />
            ))
          : null}

        {!loading && !error && data && Object.values(data.grade_counts ?? {}).every((c) => c === 0) ? (
          <View style={[st.emptyCard, { borderColor: colors.border.subtle }]}>
            <Text style={st.emptyTitle}>No HR picks available today.</Text>
            <Text style={st.emptyText}>Picks will populate once the model run completes.</Text>
          </View>
        ) : null}
      </ScrollView>

      {/* Fixed bottom parlay bar */}
      {mlbSlipItems.length >= 1 ? (
        <View style={st.parlayBar}>
          <View style={st.parlayTopRow}>
            <Text style={st.parlayTitle}>
              {mlbSlipItems.length} batter{mlbSlipItems.length !== 1 ? "s" : ""} selected
            </Text>
            <Pressable onPress={clearSlip}>
              <Text style={st.parlayClear}>✕</Text>
            </Pressable>
          </View>
          <View style={st.parlayLegs}>
            {mlbSlipItems.map((item) => (
              <Text key={item.id} style={st.parlayLegText}>
                • {item.player} — 1+ HR {fmtOdds(item.odds)}
              </Text>
            ))}
          </View>
          <View style={st.parlayBtnRow}>
            <Pressable
              style={[st.parlayBtn, !fdLink ? st.parlayBtnDisabled : null]}
              disabled={!fdLink}
              onPress={() => openUrl(fdLink)}
            >
              <Text style={st.parlayBtnText}>
                {mlbSlipItems.length === 1 ? "Bet FanDuel" : `Parlay on FanDuel (${mlbSlipItems.length} legs)`}
              </Text>
            </Pressable>
          </View>
          <Text style={st.parlayNote}>Parlay availability subject to sportsbook approval.</Text>
        </View>
      ) : null}
    </View>
  );
}

// ── Styles ──────────────────────────────────────────────────────────────────

const st = StyleSheet.create({
  screen: { flex: 1, backgroundColor: "#050A18" },
  scrollView: { flex: 1 },
  content: { padding: 16, gap: 10, paddingBottom: 40 },

  // Hero
  hero: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 16,
    backgroundColor: "#071731",
    padding: 16,
    gap: 6,
    marginBottom: 4,
  },
  eyebrow: { color: "#10B981", fontSize: 11, fontWeight: "700" },
  h1: { color: "#E9F2FF", fontSize: 22, fontWeight: "800", marginTop: 4 },
  sub: { color: "#A7C0E8", fontSize: 12, lineHeight: 17 },
  countsRow: { flexDirection: "row", flexWrap: "wrap", gap: 6, marginTop: 6 },
  countPill: {
    borderWidth: 1,
    borderRadius: 999,
    paddingHorizontal: 10,
    paddingVertical: 4,
    backgroundColor: "#0F172A",
  },
  countPillText: { fontSize: 11, fontWeight: "800" },

  // Grade section
  gradeSection: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 14,
    backgroundColor: "#0B1529",
    overflow: "hidden",
  },
  gradeSectionHeader: {
    flexDirection: "row",
    alignItems: "center",
    padding: 12,
    gap: 8,
  },
  gradeBadge: {
    borderWidth: 1,
    borderRadius: 6,
    width: 28,
    height: 24,
    alignItems: "center",
    justifyContent: "center",
  },
  gradeBadgeText: { fontSize: 11, fontWeight: "900" },
  gradeSectionTitle: { flex: 1, fontSize: 13, fontWeight: "800" },
  gradeSectionCount: { color: "#94A3B8", fontSize: 12, fontWeight: "700" },
  gradeSectionChevron: { color: "#64748B", fontSize: 14, width: 16, textAlign: "center" },
  gradeSectionBody: { paddingHorizontal: 8, paddingBottom: 8, gap: 8 },

  // Game header
  gameHeader: {
    paddingVertical: 6,
    paddingHorizontal: 4,
    marginTop: 4,
    borderBottomWidth: StyleSheet.hairlineWidth,
    borderBottomColor: "#1E293B",
  },
  gameHeaderTeams: { flexDirection: "row", alignItems: "center", gap: 5 },
  gameHeaderLogo: { width: 18, height: 18, borderRadius: 9, backgroundColor: "#111827" },
  gameHeaderTeamText: { color: "#CBD5E1", fontSize: 11, fontWeight: "800" },
  gameHeaderAt: { color: "#475569", fontSize: 10, fontWeight: "700" },
  gameHeaderTime: { color: "#64748B", fontSize: 10, marginTop: 2 },

  // Batter card
  card: {
    borderWidth: 1,
    borderColor: "#1E293B",
    borderRadius: 12,
    backgroundColor: "#0F172A",
    padding: 12,
    gap: 6,
  },
  cardSelected: {
    borderColor: "#10B981",
    backgroundColor: "rgba(16,185,129,0.06)",
  },
  cardWeakSpot: {
    borderLeftWidth: 3,
    borderLeftColor: "#10B981",
  },
  cardHeader: { gap: 3 },
  cardNameRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 6,
  },
  cardName: { color: "#E5E7EB", fontSize: 14, fontWeight: "800", flex: 1 },
  cardHand: { color: "#64748B", fontSize: 11, fontWeight: "700" },
  cardOdds: { color: "#F59E0B", fontSize: 13, fontWeight: "800" },
  cardSubRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 6,
  },
  gradePill: {
    borderWidth: 1,
    borderRadius: 999,
    paddingHorizontal: 8,
    paddingVertical: 2,
  },
  gradePillText: { fontSize: 9, fontWeight: "800" },
  cardBook: { color: "#64748B", fontSize: 9, fontWeight: "600" },
  cardVs: { color: "#94A3B8", fontSize: 10, fontWeight: "600", flex: 1 },
  cardChevron: { color: "#64748B", fontSize: 12, width: 16, textAlign: "center" },
  cardStatsRow: {
    flexDirection: "row",
    gap: 16,
    marginTop: 4,
  },
  cardBigStat: { alignItems: "center" },
  cardBigNum: { color: "#F8FAFC", fontSize: 22, fontWeight: "900" },
  cardBigLabel: { color: "#64748B", fontSize: 9, fontWeight: "700" },
  cardSecondaryRow: {
    flexDirection: "row",
    gap: 8,
  },
  cardSmStat: { alignItems: "center", flex: 1 },
  cardSmNum: { color: "#CBD5E1", fontSize: 13, fontWeight: "800" },
  cardSmLabel: { color: "#64748B", fontSize: 8, fontWeight: "700" },
  cardFlagsRow: { flexDirection: "row", flexWrap: "wrap", gap: 4 },
  cardFlag: { color: "#F59E0B", fontSize: 9, fontWeight: "700" },
  cardSelectBtn: {
    borderWidth: 1,
    borderColor: "#334155",
    borderRadius: 6,
    paddingVertical: 6,
    alignItems: "center",
    backgroundColor: "#0B1529",
  },
  cardSelectBtnActive: {
    borderColor: "#10B981",
    backgroundColor: "rgba(16,185,129,0.15)",
  },
  cardSelectBtnDisabled: { opacity: 0.3 },
  cardSelectBtnText: { color: "#64748B", fontSize: 11, fontWeight: "700" },
  cardSelectBtnTextActive: { color: "#10B981" },

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
  expandedRow: { flexDirection: "row", gap: 8, flexWrap: "wrap" },
  expandedStat: { gap: 1 },
  expandedLabel: { color: "#64748B", fontSize: 8, fontWeight: "800" },
  expandedValue: { color: "#E2E8F0", fontSize: 11, fontWeight: "700" },
  whyText: { color: "#94A3B8", fontSize: 10, fontStyle: "italic", marginTop: 2 },
  flagsRow: { flexDirection: "row", flexWrap: "wrap", gap: 4, marginTop: 2 },
  flagPill: {
    borderWidth: StyleSheet.hairlineWidth,
    borderColor: "#334155",
    borderRadius: 999,
    paddingHorizontal: 8,
    paddingVertical: 3,
    backgroundColor: "#0F172A",
  },
  flagText: { color: "#93C5FD", fontSize: 9, fontWeight: "700" },

  // Section title
  sectionTitle: { color: "#94A3B8", fontSize: 10, fontWeight: "800", marginTop: 4 },

  // Career BvP
  bvpWrap: { gap: 4 },
  bvpSummaryText: { color: "#CBD5E1", fontSize: 11, fontWeight: "700" },
  bvpEmptyText: { color: "#64748B", fontSize: 10, fontStyle: "italic" },
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
    paddingVertical: 4,
  },
  bvpValueRow: { flexDirection: "row", backgroundColor: "rgba(15,23,42,0.25)" },
  bvpValueCell: {
    flex: 1,
    color: "#E2E8F0",
    fontSize: 11,
    fontWeight: "700",
    textAlign: "center",
    paddingVertical: 5,
  },

  // Batted Ball
  bbWrap: { gap: 6, marginTop: 2 },
  bbBarsContainer: { gap: 4 },
  bbBarsRow: { flexDirection: "row", gap: 4 },
  miniBarWrap: { flex: 1, minWidth: 38, gap: 1 },
  miniBarLabel: { color: "#64748B", fontSize: 8, fontWeight: "800" },
  miniBarTrack: {
    height: 6,
    borderRadius: 3,
    backgroundColor: "rgba(51,65,85,0.4)",
    overflow: "hidden",
  },
  miniBarFill: { height: 6, borderRadius: 3 },
  miniBarValue: { color: "#CBD5E1", fontSize: 9, fontWeight: "700" },

  // Hit log
  logHeaderRow: {
    flexDirection: "row",
    borderBottomWidth: StyleSheet.hairlineWidth,
    borderBottomColor: "#334155",
    paddingBottom: 3,
    marginBottom: 2,
  },
  logHeaderCell: {
    width: 48,
    color: "#64748B",
    fontSize: 8,
    fontWeight: "800",
    textAlign: "center",
  },
  logScrollContainer: { maxHeight: 170 },
  logScrollHint: { color: "#64748B", fontSize: 9, textAlign: "center", marginTop: 2 },
  logRow: { flexDirection: "row", paddingVertical: 2 },
  logCell: {
    width: 48,
    color: "#CBD5E1",
    fontSize: 9,
    fontWeight: "600",
    textAlign: "center",
  },
  logDateCol: { width: 72 },
  logPitchCol: { width: 56 },
  logTrajCol: { width: 64 },
  logResultCol: { width: 68 },

  // Pitcher splits
  pitcherSplitWrap: { gap: 4, marginTop: 2 },

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

  // Error / empty
  errorBox: { borderWidth: StyleSheet.hairlineWidth, borderRadius: 12, backgroundColor: "#1F2937", padding: 12 },
  errorTitle: { color: "#FCA5A5", fontWeight: "700" },
  errorText: { color: "#FECACA", marginTop: 4, fontSize: 12 },
  errorRetry: { color: "#E5E7EB", marginTop: 8, fontSize: 12 },
  emptyCard: { borderWidth: StyleSheet.hairlineWidth, borderRadius: 14, backgroundColor: "#0B1529", padding: 16 },
  emptyTitle: { color: "#E5E7EB", fontWeight: "700" },
  emptyText: { color: "#A7C0E8", marginTop: 6, fontSize: 12 },
});

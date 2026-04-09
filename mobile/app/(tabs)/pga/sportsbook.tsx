import { useCallback, useMemo, useState } from "react";
import {
  ActivityIndicator,
  Linking,
  Pressable,
  ScrollView,
  StyleSheet,
  Text,
  View,
} from "react-native";
import { Stack } from "expo-router";
import { Ionicons } from "@expo/vector-icons";

import { useTheme } from "@/store/useTheme";
import { usePgaQuery } from "@/hooks/pga/usePgaQuery";
import { AutoSortableTable } from "@/components/table/AutoSortableTable";
import { PropBetslipDrawer } from "@/components/prop/PropBetslipDrawer";
import { usePropBetslip, PropSlipItem } from "@/store/usePropBetslip";
import { useBetslipDrawer } from "@/store/useBetslipDrawer";
import { useUserSettings } from "@/store/useUserSettings";
import { buildFanDuelParlay, getBuildPlatform } from "@/utils/parlayBuilder";
import type { ColumnConfig } from "@/types/schema";

// ─── Helpers ──────────────────────────────────────────────────────────────────

function fmt(v: number | null | undefined, d = 2): string {
  if (v == null) return "\u2014";
  return Number.isInteger(v) ? String(v) : v.toFixed(d);
}
function fmtOdds(v: number | null | undefined): string {
  if (v == null) return "\u2014";
  return v > 0 ? `+${v}` : String(v);
}
function fmtPct(v: number | null | undefined): string {
  if (v == null) return "\u2014";
  return `${(v * 100).toFixed(1)}%`;
}

// ─── Types ────────────────────────────────────────────────────────────────────

type MarketTypeSummary = { type: string; count: number };
type Tournament = {
  tournament_name: string;
  tournament_slug: string;
  last_scraped: string;
  total_selections: number;
  market_types: MarketTypeSummary[];
};

type Selection = {
  market_id: number;
  market_name: string;
  market_type: string;
  player_name: string;
  selection_id: number;
  odds_decimal: number | null;
  odds_american: number | null;
  handicap: number | null;
  deep_link: string | null;
  sg_total: number | null;
  sg_approach: number | null;
  sg_putting: number | null;
  sg_off_tee: number | null;
  scoring_avg: number | null;
  birdie_avg: number | null;
  l5_total_score_avg: number | null;
  l5_finish_avg: number | null;
  cut_rate_l5: number | null;
  top10_rate_l5: number | null;
  weighted_l5_score: number | null;
  form_trend_3: number | null;
  days_since_last_event: number | null;
  score_stddev: number | null;
  l5_round_scores: string | null;
  current_position: string | null;
  current_to_par: string | null;
  current_r1: number | null;
  current_r2: number | null;
  current_r3: number | null;
  current_r4: number | null;
};

type MarketGroup = {
  market_type: string;
  market_count: number;
  total_selections: number;
  markets: {
    market_name: string;
    selection_count: number;
    selections: Selection[];
  }[];
};

type MarketsResponse = {
  tournament: string;
  count: number;
  market_groups: Record<string, MarketGroup>;
};

// ─── Market type display config ───────────────────────────────────────────────

const MARKET_TYPE_LABELS: Record<string, string> = {
  outright_winner: "Outright Winner",
  top_finish: "Top Finish",
  matchup: "Matchups",
  three_ball: "3-Ball",
  make_cut: "Make/Miss Cut",
  round_leader: "Round Leader",
  player_round_score: "Round Scores",
  top_nationality: "Top Region",
  finish_specials: "Golf Specials",
  specials: "Specials",
  hole_matchup: "Hole Matchups",
  // Legacy fallbacks
  finishing_position: "Finish Position",
  round_score: "Round Score",
  hole_score: "Hole Score",
  other: "Other",
};

const MARKET_TYPE_ORDER = [
  "outright_winner",
  "top_finish",
  "matchup",
  "three_ball",
  "make_cut",
  "round_leader",
  "player_round_score",
  "top_nationality",
  "finish_specials",
  "specials",
  "hole_matchup",
  "finishing_position",
  "round_score",
  "hole_score",
  "other",
];

// ─── Column configs per market type ───────────────────────────────────────────

const OUTRIGHT_COLUMNS: ColumnConfig[] = [
  { key: "player_name", label: "Player", width: 110, isNumeric: false },
  { key: "odds_american", label: "Odds", width: 52, isNumeric: true, formatter: fmtOdds },
  { key: "sg_total", label: "SG Tot", width: 50, isNumeric: true, formatter: (v) => fmt(v, 2) },
  { key: "sg_approach", label: "SG App", width: 50, isNumeric: true, formatter: (v) => fmt(v, 2) },
  { key: "sg_putting", label: "SG Putt", width: 50, isNumeric: true, formatter: (v) => fmt(v, 2) },
  { key: "scoring_avg", label: "Avg", width: 46, isNumeric: true, formatter: (v) => fmt(v, 1) },
  { key: "l5_finish_avg", label: "L5 Fin", width: 46, isNumeric: true, formatter: (v) => fmt(v, 1) },
  { key: "cut_rate_l5", label: "Cut%", width: 46, isNumeric: true, formatter: fmtPct },
  { key: "top10_rate_l5", label: "T10%", width: 46, isNumeric: true, formatter: fmtPct },
  { key: "form_trend_3", label: "Trend", width: 46, isNumeric: true, formatter: (v) => fmt(v, 2) },
];

const TOP_FINISH_COLUMNS: ColumnConfig[] = [
  { key: "player_name", label: "Player", width: 110, isNumeric: false },
  { key: "market_name", label: "Market", width: 70, isNumeric: false },
  { key: "odds_american", label: "Odds", width: 52, isNumeric: true, formatter: fmtOdds },
  { key: "sg_total", label: "SG Tot", width: 50, isNumeric: true, formatter: (v) => fmt(v, 2) },
  { key: "l5_finish_avg", label: "L5 Fin", width: 46, isNumeric: true, formatter: (v) => fmt(v, 1) },
  { key: "top10_rate_l5", label: "T10%", width: 46, isNumeric: true, formatter: fmtPct },
  { key: "cut_rate_l5", label: "Cut%", width: 46, isNumeric: true, formatter: fmtPct },
  { key: "weighted_l5_score", label: "L5 Wt", width: 46, isNumeric: true, formatter: (v) => fmt(v, 1) },
];

const MATCHUP_COLUMNS: ColumnConfig[] = [
  { key: "player_name", label: "Player", width: 110, isNumeric: false },
  { key: "market_name", label: "Matchup", width: 90, isNumeric: false },
  { key: "odds_american", label: "Odds", width: 52, isNumeric: true, formatter: fmtOdds },
  { key: "handicap", label: "Line", width: 44, isNumeric: true, formatter: (v) => fmt(v, 1) },
  { key: "sg_total", label: "SG Tot", width: 50, isNumeric: true, formatter: (v) => fmt(v, 2) },
  { key: "l5_finish_avg", label: "L5 Fin", width: 46, isNumeric: true, formatter: (v) => fmt(v, 1) },
  { key: "form_trend_3", label: "Trend", width: 46, isNumeric: true, formatter: (v) => fmt(v, 2) },
];

const THREE_BALL_COLUMNS: ColumnConfig[] = [
  { key: "player_name", label: "Player", width: 110, isNumeric: false },
  { key: "market_name", label: "Group", width: 90, isNumeric: false },
  { key: "odds_american", label: "Odds", width: 52, isNumeric: true, formatter: fmtOdds },
  { key: "sg_total", label: "SG Tot", width: 50, isNumeric: true, formatter: (v) => fmt(v, 2) },
  { key: "sg_approach", label: "SG App", width: 50, isNumeric: true, formatter: (v) => fmt(v, 2) },
  { key: "l5_finish_avg", label: "L5 Fin", width: 46, isNumeric: true, formatter: (v) => fmt(v, 1) },
];

const ROUND_LEADER_COLUMNS: ColumnConfig[] = [
  { key: "player_name", label: "Player", width: 110, isNumeric: false },
  { key: "odds_american", label: "Odds", width: 52, isNumeric: true, formatter: fmtOdds },
  { key: "sg_total", label: "SG Tot", width: 50, isNumeric: true, formatter: (v) => fmt(v, 2) },
  { key: "scoring_avg", label: "Avg", width: 46, isNumeric: true, formatter: (v) => fmt(v, 1) },
  { key: "l5_finish_avg", label: "L5 Fin", width: 46, isNumeric: true, formatter: (v) => fmt(v, 1) },
  { key: "cut_rate_l5", label: "Cut%", width: 46, isNumeric: true, formatter: fmtPct },
];

const PLAYER_ROUND_SCORE_COLUMNS: ColumnConfig[] = [
  { key: "player_name", label: "Player", width: 110, isNumeric: false },
  { key: "market_name", label: "Market", width: 80, isNumeric: false },
  { key: "odds_american", label: "Odds", width: 52, isNumeric: true, formatter: fmtOdds },
  { key: "sg_total", label: "SG Tot", width: 50, isNumeric: true, formatter: (v) => fmt(v, 2) },
  { key: "l5_finish_avg", label: "L5 Fin", width: 46, isNumeric: true, formatter: (v) => fmt(v, 1) },
];

const TOP_NATIONALITY_COLUMNS: ColumnConfig[] = [
  { key: "player_name", label: "Player", width: 110, isNumeric: false },
  { key: "market_name", label: "Region", width: 80, isNumeric: false },
  { key: "odds_american", label: "Odds", width: 52, isNumeric: true, formatter: fmtOdds },
  { key: "sg_total", label: "SG Tot", width: 50, isNumeric: true, formatter: (v) => fmt(v, 2) },
  { key: "l5_finish_avg", label: "L5 Fin", width: 46, isNumeric: true, formatter: (v) => fmt(v, 1) },
  { key: "cut_rate_l5", label: "Cut%", width: 46, isNumeric: true, formatter: fmtPct },
];

const SPECIALS_COLUMNS: ColumnConfig[] = [
  { key: "player_name", label: "Player", width: 110, isNumeric: false },
  { key: "market_name", label: "Market", width: 90, isNumeric: false },
  { key: "odds_american", label: "Odds", width: 52, isNumeric: true, formatter: fmtOdds },
  { key: "sg_total", label: "SG Tot", width: 50, isNumeric: true, formatter: (v) => fmt(v, 2) },
  { key: "l5_finish_avg", label: "L5 Fin", width: 46, isNumeric: true, formatter: (v) => fmt(v, 1) },
];

const GENERIC_COLUMNS: ColumnConfig[] = [
  { key: "player_name", label: "Player", width: 110, isNumeric: false },
  { key: "market_name", label: "Market", width: 90, isNumeric: false },
  { key: "odds_american", label: "Odds", width: 52, isNumeric: true, formatter: fmtOdds },
  { key: "sg_total", label: "SG Tot", width: 50, isNumeric: true, formatter: (v) => fmt(v, 2) },
  { key: "l5_finish_avg", label: "L5 Fin", width: 46, isNumeric: true, formatter: (v) => fmt(v, 1) },
  { key: "cut_rate_l5", label: "Cut%", width: 46, isNumeric: true, formatter: fmtPct },
];

function columnsForType(mt: string): ColumnConfig[] {
  if (mt === "outright_winner") return OUTRIGHT_COLUMNS;
  if (mt === "top_finish") return TOP_FINISH_COLUMNS;
  if (mt === "matchup") return MATCHUP_COLUMNS;
  if (mt === "three_ball") return THREE_BALL_COLUMNS;
  if (mt === "round_leader") return ROUND_LEADER_COLUMNS;
  if (mt === "player_round_score") return PLAYER_ROUND_SCORE_COLUMNS;
  if (mt === "top_nationality") return TOP_NATIONALITY_COLUMNS;
  if (mt === "specials" || mt === "finish_specials") return SPECIALS_COLUMNS;
  if (mt === "hole_matchup") return MATCHUP_COLUMNS;
  if (mt === "make_cut") return GENERIC_COLUMNS;
  if (mt === "finishing_position") return TOP_FINISH_COLUMNS;
  return GENERIC_COLUMNS;
}

// ─── Selection key helpers ───────────────────────────────────────────────────

function selectionKey(row: Selection): string {
  return `pga-${row.market_id}-${row.selection_id}`;
}

function selectionToSlipItem(row: Selection): PropSlipItem {
  return {
    id: selectionKey(row),
    player_id: row.selection_id ?? 0,
    player: row.player_name,
    market: row.market_name,
    side: "over",
    line: row.handicap ?? 0,
    odds: row.odds_american ?? 0,
    sport: "pga",
    fd_market_id: String(row.market_id ?? ""),
    fd_selection_id: String(row.selection_id ?? ""),
  };
}

// ─── Group Comparison (Matchup / 3-Ball) ─────────────────────────────────────

type GroupStat = { label: string; value: string };

function statRows(sel: Selection, marketType: string): GroupStat[] {
  if (marketType === "matchup") {
    return [
      { label: "SG Tot", value: fmt(sel.sg_total, 2) },
      { label: "L5 Fin", value: fmt(sel.l5_finish_avg, 1) },
      { label: "Trend", value: fmt(sel.form_trend_3, 2) },
      { label: "Cut%", value: fmtPct(sel.cut_rate_l5) },
    ];
  }
  // three_ball — fewer stats so cards stay compact at 3-wide
  return [
    { label: "SG Tot", value: fmt(sel.sg_total, 2) },
    { label: "L5 Fin", value: fmt(sel.l5_finish_avg, 1) },
    { label: "Cut%", value: fmtPct(sel.cut_rate_l5) },
  ];
}

function fmtRounds(csv: string | null): string {
  if (!csv) return "\u2014";
  return csv.split(",").join(", ");
}

function fmtCurrentScore(sel: Selection): string | null {
  const parts: string[] = [];
  if (sel.current_r1 != null) parts.push(String(sel.current_r1));
  if (sel.current_r2 != null) parts.push(String(sel.current_r2));
  if (sel.current_r3 != null) parts.push(String(sel.current_r3));
  if (sel.current_r4 != null) parts.push(String(sel.current_r4));
  if (!parts.length) return null;
  const pos = sel.current_position ?? "";
  const par = sel.current_to_par ?? "";
  return `${parts.join("-")} (${par}) ${pos}`.trim();
}

function bestIndex(sels: Selection[], key: keyof Selection, lower = false): number {
  let best = -1;
  let bestVal = lower ? Infinity : -Infinity;
  for (let i = 0; i < sels.length; i++) {
    const v = sels[i][key] as number | null;
    if (v == null) continue;
    if (lower ? v < bestVal : v > bestVal) {
      bestVal = v;
      best = i;
    }
  }
  return best;
}

function GroupCard({
  market,
  marketType,
  selectedKeys,
  onToggle,
  onPlayerPress,
}: {
  market: { market_name: string; selections: Selection[] };
  marketType: string;
  selectedKeys: Set<string>;
  onToggle: (key: string, row: Selection) => void;
  onPlayerPress: (row: Selection) => void;
}) {
  const { colors } = useTheme();
  const sels = market.selections;
  const is3 = sels.length >= 3;
  const bestOddsIdx = bestIndex(sels, "odds_american", false);
  const bestSgIdx = bestIndex(sels, "sg_total", false);
  const bestFinIdx = bestIndex(sels, "l5_finish_avg", true);

  return (
    <View
      style={[
        gStyles.card,
        { borderColor: colors.border.subtle, backgroundColor: "#0B1529" },
      ]}
    >
      <Text style={[gStyles.cardTitle, { color: colors.text.muted }]}>
        {market.market_name}
      </Text>

      <View style={gStyles.playersRow}>
        {sels.map((sel, i) => {
          const key = selectionKey(sel);
          const selected = selectedKeys.has(key);
          const stats = statRows(sel, marketType);
          const currentScore = fmtCurrentScore(sel);

          return (
            <Pressable
              key={key}
              onPress={() => onPlayerPress(sel)}
              style={[
                gStyles.playerCol,
                {
                  flex: 1,
                  width: 0, // force flex shrink on RN
                  borderColor: selected ? "#4ADE80" : colors.border.subtle,
                  backgroundColor: selected ? "rgba(74,222,128,0.08)" : "#0F1D32",
                },
              ]}
            >
              {/* Checkbox + Name */}
              <View style={gStyles.nameRow}>
                <Pressable onPress={() => onToggle(key, sel)} hitSlop={8}>
                  <Text style={{ fontSize: is3 ? 12 : 14, color: selected ? "#4ADE80" : colors.text.muted }}>
                    {selected ? "\u2611" : "\u2610"}
                  </Text>
                </Pressable>
                <Text
                  numberOfLines={1}
                  style={[
                    gStyles.playerName,
                    { color: colors.text.primary, fontSize: is3 ? 11 : 13 },
                  ]}
                >
                  {sel.player_name}
                </Text>
              </View>

              {/* Odds */}
              <Text
                style={[
                  gStyles.odds,
                  {
                    fontSize: is3 ? 16 : 20,
                    color: i === bestOddsIdx ? "#4ADE80" : colors.text.secondary,
                  },
                ]}
              >
                {fmtOdds(sel.odds_american)}
              </Text>

              {/* Handicap for matchups */}
              {marketType === "matchup" && sel.handicap != null && (
                <Text style={[gStyles.handicap, { color: colors.text.muted }]}>
                  Line: {fmt(sel.handicap, 1)}
                </Text>
              )}

              {/* Current tournament score */}
              {currentScore && (
                <Text
                  numberOfLines={1}
                  style={[gStyles.roundScores, { color: "#60A5FA" }]}
                >
                  Now: {currentScore}
                </Text>
              )}

              {/* Last 5 round scores */}
              <Text
                numberOfLines={1}
                style={[gStyles.roundScores, { color: colors.text.muted }]}
              >
                L5: {fmtRounds(sel.l5_round_scores)}
              </Text>

              {/* Stats */}
              {stats.map((st) => (
                <View key={st.label} style={gStyles.statRow}>
                  <Text
                    style={[
                      gStyles.statLabel,
                      { color: colors.text.muted, fontSize: is3 ? 10 : 11 },
                    ]}
                  >
                    {st.label}
                  </Text>
                  <Text
                    style={[
                      gStyles.statValue,
                      {
                        fontSize: is3 ? 10 : 11,
                        color:
                          (st.label === "SG Tot" && i === bestSgIdx) ||
                          (st.label === "L5 Fin" && i === bestFinIdx)
                            ? "#4ADE80"
                            : colors.text.primary,
                      },
                    ]}
                  >
                    {st.value}
                  </Text>
                </View>
              ))}
            </Pressable>
          );
        })}
      </View>
    </View>
  );
}

function GroupComparisonView({
  markets,
  marketType,
  selectedKeys,
  onToggle,
  onPlayerPress,
}: {
  markets: { market_name: string; selection_count: number; selections: Selection[] }[];
  marketType: string;
  selectedKeys: Set<string>;
  onToggle: (key: string, row: Selection) => void;
  onPlayerPress: (row: Selection) => void;
}) {
  return (
    <ScrollView
      style={{ flex: 1 }}
      contentContainerStyle={{ padding: 12, paddingBottom: 24, gap: 10 }}
      showsVerticalScrollIndicator={false}
    >
      {markets.map((m) => (
        <GroupCard
          key={m.market_name}
          market={m}
          marketType={marketType}
          selectedKeys={selectedKeys}
          onToggle={onToggle}
          onPlayerPress={onPlayerPress}
        />
      ))}
    </ScrollView>
  );
}

const gStyles = StyleSheet.create({
  card: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 14,
    padding: 12,
    overflow: "hidden",
  },
  cardTitle: {
    fontSize: 12,
    fontWeight: "700",
    marginBottom: 8,
    textAlign: "center",
  },
  playersRow: {
    flexDirection: "row",
    gap: 6,
  },
  playerCol: {
    borderWidth: 1,
    borderRadius: 10,
    padding: 8,
    gap: 3,
  },
  nameRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 6,
    marginBottom: 2,
  },
  playerName: {
    fontSize: 13,
    fontWeight: "800",
    flex: 1,
  },
  odds: {
    fontSize: 20,
    fontWeight: "900",
    textAlign: "center",
    marginVertical: 4,
  },
  handicap: {
    fontSize: 11,
    textAlign: "center",
    marginBottom: 4,
  },
  statRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    paddingVertical: 2,
  },
  roundScores: {
    fontSize: 10,
    fontWeight: "600",
    textAlign: "center",
    marginVertical: 2,
  },
  statLabel: {
    fontSize: 11,
    fontWeight: "600",
  },
  statValue: {
    fontSize: 11,
    fontWeight: "700",
  },
});

// ─── Tournament Card ──────────────────────────────────────────────────────────

function TournamentCard({
  tournament,
  onPress,
}: {
  tournament: Tournament;
  onPress: () => void;
}) {
  const { colors } = useTheme();
  const scraped = tournament.last_scraped
    ? new Date(tournament.last_scraped).toLocaleString(undefined, {
        month: "short",
        day: "numeric",
        hour: "numeric",
        minute: "2-digit",
      })
    : "\u2014";

  return (
    <Pressable
      onPress={onPress}
      style={[
        styles.tournCard,
        { borderColor: colors.border.subtle, backgroundColor: "#0B1529" },
      ]}
    >
      <View style={styles.tournCardGlow} />
      <View style={styles.tournHeader}>
        <View style={{ flex: 1 }}>
          <Text style={[styles.tournName, { color: colors.text.primary }]}>
            {tournament.tournament_name}
          </Text>
          <Text style={[styles.tournMeta, { color: colors.text.muted }]}>
            {tournament.total_selections} selections \u00B7 scraped {scraped}
          </Text>
        </View>
        <Ionicons name="chevron-forward" size={18} color={colors.text.muted} />
      </View>
      <View style={styles.marketBadgeRow}>
        {tournament.market_types.slice(0, 8).map((mt) => (
          <View
            key={mt.type}
            style={[styles.marketBadge, { backgroundColor: "rgba(74,222,128,0.12)" }]}
          >
            <Text style={styles.marketBadgeText}>
              {MARKET_TYPE_LABELS[mt.type] || mt.type} ({mt.count})
            </Text>
          </View>
        ))}
      </View>
    </Pressable>
  );
}

// ─── Main Screen ──────────────────────────────────────────────────────────────

export default function PgaSportsbook() {
  const { colors } = useTheme();
  const [selectedTournament, setSelectedTournament] = useState<string | null>(null);
  const [activeMarketType, setActiveMarketType] = useState<string | null>(null);
  const [activeSubFilter, setActiveSubFilter] = useState<string | null>(null);

  // Betslip stores
  const { items: betslipItems, add: addToBetslip, remove: removeFromBetslip } = usePropBetslip();
  const { open: openDrawer } = useBetslipDrawer();
  const { fdState } = useUserSettings();
  const platform = getBuildPlatform();

  // Selected keys from betslip (PGA items only)
  const selectedKeys = useMemo(() => {
    const keys = new Set<string>();
    for (const item of betslipItems) {
      if (item.sport === "pga") keys.add(item.id);
    }
    return keys;
  }, [betslipItems]);

  // Toggle selection
  const handleToggle = useCallback(
    (key: string, row: Selection) => {
      if (selectedKeys.has(key)) {
        removeFromBetslip(key);
      } else {
        addToBetslip(selectionToSlipItem(row));
        openDrawer();
      }
    },
    [selectedKeys, addToBetslip, removeFromBetslip, openDrawer],
  );

  // Open FanDuel NJ deep link for a single selection
  const handleRowPress = useCallback(
    (row: Selection) => {
      if (!row.market_id || !row.selection_id) {
        if (row.deep_link) Linking.openURL(row.deep_link);
        return;
      }
      const link = buildFanDuelParlay(
        [{ fd_market_id: String(row.market_id), fd_selection_id: String(row.selection_id) }],
        platform,
        fdState,
      );
      if (link) {
        if (platform === "desktop" && typeof globalThis.open === "function") {
          globalThis.open(link, "_blank");
        } else {
          Linking.openURL(link).catch(() => {});
        }
      } else if (row.deep_link) {
        Linking.openURL(row.deep_link);
      }
    },
    [platform, fdState],
  );

  // Fetch tournament list
  const {
    data: tournaments,
    loading: tournsLoading,
    error: tournsError,
    refetch: refetchTourns,
  } = usePgaQuery<Tournament[]>("/pga/sportsbook/tournaments");

  // Fetch markets for selected tournament
  const {
    data: marketsData,
    loading: marketsLoading,
    error: marketsError,
  } = usePgaQuery<MarketsResponse>(
    "/pga/sportsbook/markets",
    { tournament: selectedTournament ?? "" },
    !!selectedTournament,
  );

  // Sorted market type tabs for the selected tournament
  const marketTypeTabs = useMemo(() => {
    if (!marketsData?.market_groups) return [];
    const keys = Object.keys(marketsData.market_groups);
    return MARKET_TYPE_ORDER.filter((mt) => keys.includes(mt)).concat(
      keys.filter((k) => !MARKET_TYPE_ORDER.includes(k)),
    );
  }, [marketsData]);

  // Auto-select first market type when tournament loads
  const effectiveTab =
    activeMarketType && marketTypeTabs.includes(activeMarketType)
      ? activeMarketType
      : marketTypeTabs[0] ?? null;

  // Sub-filter options: distinct market_names within the active tab
  const subFilterOptions = useMemo(() => {
    if (!marketsData?.market_groups || !effectiveTab) return [];
    const group = marketsData.market_groups[effectiveTab];
    if (!group || group.markets.length <= 1) return [];
    return group.markets.map((m) => m.market_name);
  }, [marketsData, effectiveTab]);

  // Effective sub-filter — reset to "All" when options change
  const effectiveSubFilter =
    activeSubFilter && subFilterOptions.includes(activeSubFilter)
      ? activeSubFilter
      : null; // null = "All"

  // Flatten selections for the active tab, filtered by sub-filter
  const tableData = useMemo(() => {
    if (!marketsData?.market_groups || !effectiveTab) return [];
    const group = marketsData.market_groups[effectiveTab];
    if (!group) return [];
    if (effectiveSubFilter) {
      const filtered = group.markets.filter((m) => m.market_name === effectiveSubFilter);
      return filtered.flatMap((m) => m.selections);
    }
    return group.markets.flatMap((m) => m.selections);
  }, [marketsData, effectiveTab, effectiveSubFilter]);

  // Filtered markets for GroupComparisonView (matchup/3-ball)
  const filteredMarkets = useMemo(() => {
    if (!marketsData?.market_groups || !effectiveTab) return [];
    const group = marketsData.market_groups[effectiveTab];
    if (!group) return [];
    if (effectiveSubFilter) {
      return group.markets.filter((m) => m.market_name === effectiveSubFilter);
    }
    return group.markets;
  }, [marketsData, effectiveTab, effectiveSubFilter]);

  // ─── Tournament List View ────────────────────────────────────────────────

  if (!selectedTournament) {
    return (
      <ScrollView style={styles.screen} contentContainerStyle={styles.content}>
        <Stack.Screen options={{ title: "PGA" }} />

        <View style={[styles.hero, { borderColor: colors.border.subtle }]}>
          <View style={styles.heroGlow} />
          <Text style={styles.eyebrow}>FANDUEL ODDS</Text>
          <Text style={styles.h1}>Upcoming Tournaments</Text>
          <Text style={[styles.sub, { color: colors.text.muted }]}>
            All available PGA Tour tournament odds from FanDuel, updated daily.
          </Text>
        </View>

        {tournsLoading && (
          <ActivityIndicator size="large" color={colors.accent.primary} style={{ marginTop: 32 }} />
        )}
        {tournsError && (
          <Text style={[styles.errorText, { color: "#F87171" }]}>{tournsError}</Text>
        )}
        {tournaments?.map((t) => (
          <TournamentCard
            key={t.tournament_name}
            tournament={t}
            onPress={() => {
              setSelectedTournament(t.tournament_name);
              setActiveMarketType(null);
            }}
          />
        ))}
        {!tournsLoading && tournaments?.length === 0 && (
          <Text style={[styles.emptyText, { color: colors.text.muted }]}>
            No tournament odds available. Check back Monday.
          </Text>
        )}
      </ScrollView>
    );
  }

  // ─── Tournament Detail View ──────────────────────────────────────────────

  return (
    <View style={styles.screen}>
      <Stack.Screen options={{ title: selectedTournament }} />

      {/* Back button */}
      <Pressable
        onPress={() => {
          setSelectedTournament(null);
          setActiveMarketType(null);
          setActiveSubFilter(null);
        }}
        style={styles.backBtn}
      >
        <Ionicons name="chevron-back" size={18} color="#90B3E9" />
        <Text style={styles.backText}>All Tournaments</Text>
      </Pressable>

      {/* Selection count badge */}
      {selectedKeys.size > 0 && (
        <View style={styles.selectionBanner}>
          <Text style={styles.selectionBannerText}>
            {selectedKeys.size} bet{selectedKeys.size !== 1 ? "s" : ""} selected
          </Text>
        </View>
      )}

      {/* Market type tabs */}
      <ScrollView
        horizontal
        showsHorizontalScrollIndicator={false}
        contentContainerStyle={styles.tabBar}
        style={{ flexGrow: 0 }}
      >
        {marketTypeTabs.map((mt) => {
          const active = mt === effectiveTab;
          return (
            <Pressable
              key={mt}
              onPress={() => { setActiveMarketType(mt); setActiveSubFilter(null); }}
              style={[
                styles.tab,
                active && {
                  backgroundColor: colors.accent.primary,
                  borderColor: colors.accent.primary,
                },
                !active && { borderColor: colors.border.subtle },
              ]}
            >
              <Text
                style={[
                  styles.tabLabel,
                  { color: active ? colors.text.inverse : colors.text.muted },
                ]}
              >
                {MARKET_TYPE_LABELS[mt] || mt}
              </Text>
            </Pressable>
          );
        })}
      </ScrollView>

      {/* Sub-filter toggles (market_name within a tab) */}
      {!marketsLoading && subFilterOptions.length > 1 && (
        <ScrollView
          horizontal
          showsHorizontalScrollIndicator={false}
          contentContainerStyle={[styles.tabBar, { paddingTop: 0, paddingBottom: 6 }]}
          style={{ flexGrow: 0 }}
        >
          <Pressable
            onPress={() => setActiveSubFilter(null)}
            style={[
              styles.subTab,
              !effectiveSubFilter
                ? { backgroundColor: colors.accent.primary, borderColor: colors.accent.primary }
                : { borderColor: colors.border.subtle },
            ]}
          >
            <Text style={[styles.subTabLabel, { color: !effectiveSubFilter ? colors.text.inverse : colors.text.muted }]}>
              All
            </Text>
          </Pressable>
          {subFilterOptions.map((name) => {
            const active = name === effectiveSubFilter;
            return (
              <Pressable
                key={name}
                onPress={() => setActiveSubFilter(name)}
                style={[
                  styles.subTab,
                  active
                    ? { backgroundColor: colors.accent.primary, borderColor: colors.accent.primary }
                    : { borderColor: colors.border.subtle },
                ]}
              >
                <Text
                  numberOfLines={1}
                  style={[styles.subTabLabel, { color: active ? colors.text.inverse : colors.text.muted }]}
                >
                  {name}
                </Text>
              </Pressable>
            );
          })}
        </ScrollView>
      )}

      {/* Table */}
      {marketsLoading && (
        <ActivityIndicator size="large" color={colors.accent.primary} style={{ marginTop: 32 }} />
      )}
      {marketsError && (
        <Text style={[styles.errorText, { color: "#F87171" }]}>{marketsError}</Text>
      )}
      {!marketsLoading && effectiveTab && tableData.length > 0 && (
        <View style={{ flex: 1, paddingBottom: selectedKeys.size > 0 ? 80 : 0 }}>
          {(effectiveTab === "matchup" || effectiveTab === "three_ball" || effectiveTab === "hole_matchup") &&
           marketsData?.market_groups[effectiveTab] ? (
            <GroupComparisonView
              markets={filteredMarkets}
              marketType={effectiveTab}
              selectedKeys={selectedKeys}
              onToggle={handleToggle}
              onPlayerPress={handleRowPress}
            />
          ) : (
            <AutoSortableTable
              data={tableData}
              columns={columnsForType(effectiveTab)}
              defaultSort="odds_american"
              autoWidth
              selectable
              selectedKeys={selectedKeys}
              onToggle={handleToggle}
              rowKey={selectionKey}
              onRowPress={handleRowPress}
            />
          )}
        </View>
      )}
      {!marketsLoading && effectiveTab && tableData.length === 0 && (
        <Text style={[styles.emptyText, { color: colors.text.muted }]}>
          No selections available for this market type.
        </Text>
      )}

      <PropBetslipDrawer />
    </View>
  );
}

// ─── Styles ───────────────────────────────────────────────────────────────────

const styles = StyleSheet.create({
  screen: { flex: 1, backgroundColor: "#050A18" },
  content: { padding: 16, paddingBottom: 44 },
  hero: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 18,
    padding: 16,
    backgroundColor: "#071731",
    overflow: "hidden",
    marginBottom: 12,
  },
  heroGlow: {
    position: "absolute",
    right: -72,
    top: -64,
    width: 220,
    height: 220,
    borderRadius: 999,
    backgroundColor: "rgba(74,222,128,0.12)",
  },
  eyebrow: {
    color: "#90B3E9",
    fontSize: 11,
    fontWeight: "700",
    letterSpacing: 1,
  },
  h1: {
    marginTop: 8,
    fontSize: 26,
    fontWeight: "800",
    color: "#E9F2FF",
  },
  sub: { marginTop: 6, fontSize: 13, lineHeight: 19 },
  tournCard: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 14,
    padding: 14,
    marginTop: 10,
    overflow: "hidden",
  },
  tournCardGlow: {
    position: "absolute",
    top: -46,
    right: -42,
    width: 120,
    height: 120,
    borderRadius: 999,
    backgroundColor: "rgba(74,222,128,0.08)",
  },
  tournHeader: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
  },
  tournName: { fontSize: 17, fontWeight: "800" },
  tournMeta: { fontSize: 12, marginTop: 4 },
  marketBadgeRow: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 6,
    marginTop: 10,
  },
  marketBadge: {
    borderRadius: 8,
    paddingHorizontal: 8,
    paddingVertical: 4,
  },
  marketBadgeText: { fontSize: 11, fontWeight: "600", color: "#4ADE80" },
  backBtn: {
    flexDirection: "row",
    alignItems: "center",
    gap: 4,
    paddingHorizontal: 16,
    paddingTop: 12,
    paddingBottom: 4,
  },
  backText: { color: "#90B3E9", fontSize: 14, fontWeight: "600" },
  selectionBanner: {
    marginHorizontal: 12,
    marginTop: 4,
    paddingVertical: 4,
    paddingHorizontal: 10,
    backgroundColor: "rgba(74,222,128,0.15)",
    borderRadius: 8,
    alignSelf: "flex-start",
  },
  selectionBannerText: {
    color: "#4ADE80",
    fontSize: 11,
    fontWeight: "700",
  },
  tabBar: { paddingHorizontal: 12, paddingVertical: 4, gap: 5 },
  tab: {
    borderWidth: 1,
    borderRadius: 12,
    paddingHorizontal: 8,
    paddingVertical: 3,
  },
  tabLabel: { fontSize: 10, fontWeight: "700" },
  subTab: {
    borderWidth: 1,
    borderRadius: 10,
    paddingHorizontal: 7,
    paddingVertical: 2,
  },
  subTabLabel: { fontSize: 9, fontWeight: "600" },
  errorText: { textAlign: "center", marginTop: 24, fontSize: 13 },
  emptyText: { textAlign: "center", marginTop: 32, fontSize: 14 },
});

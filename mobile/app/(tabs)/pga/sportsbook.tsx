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
  finishing_position: "Top Finish",
  matchup: "Matchups",
  three_ball: "3-Ball",
  top_nationality: "Top Nationality",
  make_cut: "Make/Miss Cut",
  first_round_leader: "Round Leader",
  round_score: "Round Score",
  hole_score: "Hole Score",
  other: "Other",
};

const MARKET_TYPE_ORDER = [
  "outright_winner",
  "top_finish",
  "finishing_position",
  "matchup",
  "three_ball",
  "top_nationality",
  "make_cut",
  "first_round_leader",
  "round_score",
  "hole_score",
  "other",
];

// ─── Column configs per market type ───────────────────────────────────────────

const OUTRIGHT_COLUMNS: ColumnConfig[] = [
  { key: "player_name", label: "Player", width: 140, isNumeric: false },
  { key: "odds_american", label: "Odds", width: 68, isNumeric: true, formatter: fmtOdds },
  { key: "sg_total", label: "SG Tot", width: 60, isNumeric: true, formatter: (v) => fmt(v, 2) },
  { key: "sg_approach", label: "SG App", width: 60, isNumeric: true, formatter: (v) => fmt(v, 2) },
  { key: "sg_putting", label: "SG Putt", width: 60, isNumeric: true, formatter: (v) => fmt(v, 2) },
  { key: "scoring_avg", label: "Scr Avg", width: 64, isNumeric: true, formatter: (v) => fmt(v, 1) },
  { key: "l5_finish_avg", label: "L5 Fin", width: 56, isNumeric: true, formatter: (v) => fmt(v, 1) },
  { key: "cut_rate_l5", label: "Cut%", width: 52, isNumeric: true, formatter: fmtPct },
  { key: "top10_rate_l5", label: "T10%", width: 52, isNumeric: true, formatter: fmtPct },
  { key: "form_trend_3", label: "Trend", width: 52, isNumeric: true, formatter: (v) => fmt(v, 2) },
];

const TOP_FINISH_COLUMNS: ColumnConfig[] = [
  { key: "player_name", label: "Player", width: 140, isNumeric: false },
  { key: "market_name", label: "Market", width: 90, isNumeric: false },
  { key: "odds_american", label: "Odds", width: 68, isNumeric: true, formatter: fmtOdds },
  { key: "sg_total", label: "SG Tot", width: 60, isNumeric: true, formatter: (v) => fmt(v, 2) },
  { key: "l5_finish_avg", label: "L5 Fin", width: 56, isNumeric: true, formatter: (v) => fmt(v, 1) },
  { key: "top10_rate_l5", label: "T10%", width: 52, isNumeric: true, formatter: fmtPct },
  { key: "cut_rate_l5", label: "Cut%", width: 52, isNumeric: true, formatter: fmtPct },
  { key: "weighted_l5_score", label: "L5 Wt", width: 56, isNumeric: true, formatter: (v) => fmt(v, 1) },
];

const MATCHUP_COLUMNS: ColumnConfig[] = [
  { key: "player_name", label: "Player", width: 140, isNumeric: false },
  { key: "market_name", label: "Matchup", width: 120, isNumeric: false },
  { key: "odds_american", label: "Odds", width: 68, isNumeric: true, formatter: fmtOdds },
  { key: "handicap", label: "Line", width: 52, isNumeric: true, formatter: (v) => fmt(v, 1) },
  { key: "sg_total", label: "SG Tot", width: 60, isNumeric: true, formatter: (v) => fmt(v, 2) },
  { key: "l5_finish_avg", label: "L5 Fin", width: 56, isNumeric: true, formatter: (v) => fmt(v, 1) },
  { key: "form_trend_3", label: "Trend", width: 52, isNumeric: true, formatter: (v) => fmt(v, 2) },
];

const THREE_BALL_COLUMNS: ColumnConfig[] = [
  { key: "player_name", label: "Player", width: 140, isNumeric: false },
  { key: "market_name", label: "Group", width: 120, isNumeric: false },
  { key: "odds_american", label: "Odds", width: 68, isNumeric: true, formatter: fmtOdds },
  { key: "sg_total", label: "SG Tot", width: 60, isNumeric: true, formatter: (v) => fmt(v, 2) },
  { key: "sg_approach", label: "SG App", width: 60, isNumeric: true, formatter: (v) => fmt(v, 2) },
  { key: "l5_finish_avg", label: "L5 Fin", width: 56, isNumeric: true, formatter: (v) => fmt(v, 1) },
];

const GENERIC_COLUMNS: ColumnConfig[] = [
  { key: "player_name", label: "Player", width: 140, isNumeric: false },
  { key: "market_name", label: "Market", width: 120, isNumeric: false },
  { key: "odds_american", label: "Odds", width: 68, isNumeric: true, formatter: fmtOdds },
  { key: "sg_total", label: "SG Tot", width: 60, isNumeric: true, formatter: (v) => fmt(v, 2) },
  { key: "l5_finish_avg", label: "L5 Fin", width: 56, isNumeric: true, formatter: (v) => fmt(v, 1) },
  { key: "cut_rate_l5", label: "Cut%", width: 52, isNumeric: true, formatter: fmtPct },
];

function columnsForType(mt: string): ColumnConfig[] {
  if (mt === "outright_winner") return OUTRIGHT_COLUMNS;
  if (mt === "top_finish" || mt === "finishing_position") return TOP_FINISH_COLUMNS;
  if (mt === "matchup") return MATCHUP_COLUMNS;
  if (mt === "three_ball") return THREE_BALL_COLUMNS;
  return GENERIC_COLUMNS;
}

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
        {tournament.market_types.slice(0, 5).map((mt) => (
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
    !!selectedTournament
  );

  // Sorted market type tabs for the selected tournament
  const marketTypeTabs = useMemo(() => {
    if (!marketsData?.market_groups) return [];
    const keys = Object.keys(marketsData.market_groups);
    return MARKET_TYPE_ORDER.filter((mt) => keys.includes(mt)).concat(
      keys.filter((k) => !MARKET_TYPE_ORDER.includes(k))
    );
  }, [marketsData]);

  // Auto-select first market type when tournament loads
  const effectiveTab = activeMarketType && marketTypeTabs.includes(activeMarketType)
    ? activeMarketType
    : marketTypeTabs[0] ?? null;

  // Flatten selections for the active tab
  const tableData = useMemo(() => {
    if (!marketsData?.market_groups || !effectiveTab) return [];
    const group = marketsData.market_groups[effectiveTab];
    if (!group) return [];
    return group.markets.flatMap((m) => m.selections);
  }, [marketsData, effectiveTab]);

  // ─── Tournament List View ────────────────────────────────────────────────

  if (!selectedTournament) {
    return (
      <ScrollView style={styles.screen} contentContainerStyle={styles.content}>
        <Stack.Screen options={{ title: "PGA Sportsbook" }} />

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
        }}
        style={styles.backBtn}
      >
        <Ionicons name="chevron-back" size={18} color="#90B3E9" />
        <Text style={styles.backText}>All Tournaments</Text>
      </Pressable>

      {/* Market type tabs */}
      <ScrollView
        horizontal
        showsHorizontalScrollIndicator={false}
        contentContainerStyle={styles.tabBar}
      >
        {marketTypeTabs.map((mt) => {
          const active = mt === effectiveTab;
          return (
            <Pressable
              key={mt}
              onPress={() => setActiveMarketType(mt)}
              style={[
                styles.tab,
                active && { backgroundColor: colors.accent.primary, borderColor: colors.accent.primary },
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

      {/* Table */}
      {marketsLoading && (
        <ActivityIndicator size="large" color={colors.accent.primary} style={{ marginTop: 32 }} />
      )}
      {marketsError && (
        <Text style={[styles.errorText, { color: "#F87171" }]}>{marketsError}</Text>
      )}
      {!marketsLoading && effectiveTab && tableData.length > 0 && (
        <View style={{ flex: 1 }}>
          <AutoSortableTable
            data={tableData}
            columns={columnsForType(effectiveTab)}
            onRowPress={(row) => {
              if (row.deep_link) Linking.openURL(row.deep_link);
            }}
          />
        </View>
      )}
      {!marketsLoading && effectiveTab && tableData.length === 0 && (
        <Text style={[styles.emptyText, { color: colors.text.muted }]}>
          No selections available for this market type.
        </Text>
      )}
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
  tabBar: { paddingHorizontal: 12, paddingVertical: 4, gap: 5 },
  tab: {
    borderWidth: 1,
    borderRadius: 12,
    paddingHorizontal: 8,
    paddingVertical: 3,
  },
  tabLabel: { fontSize: 10, fontWeight: "700" },
  errorText: { textAlign: "center", marginTop: 24, fontSize: 13 },
  emptyText: { textAlign: "center", marginTop: 32, fontSize: 14 },
});

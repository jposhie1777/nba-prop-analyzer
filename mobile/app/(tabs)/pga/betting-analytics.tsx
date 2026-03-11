import {
  ActivityIndicator,
  Pressable,
  ScrollView,
  StyleSheet,
  Text,
  View,
} from "react-native";
import { Stack } from "expo-router";
import { useMemo, useState } from "react";

import { useTheme } from "@/store/useTheme";
import { usePgaQuery } from "@/hooks/pga/usePgaQuery";
import { usePgaBetslip } from "@/store/usePgaBetslip";
import { usePgaBetslipDrawer } from "@/store/usePgaBetslipDrawer";
import { PgaBetslipDrawer } from "@/components/pga/PgaBetslipDrawer";
import { AutoSortableTable } from "@/components/table/AutoSortableTable";
import type { ColumnConfig } from "@/types/schema";
import type {
  PgaBettingOutrightRow,
  PgaBettingFinishRow,
  PgaBettingMatchupRow,
  PgaBetting3BallRow,
  PgaPlayerSkillStatsRow,
  PgaRecentFormRow,
} from "@/types/pga";

// ─── Helpers ──────────────────────────────────────────────────────────────────

function fmt(value: number | null | undefined, digits = 2): string {
  if (value == null) return "—";
  return Number.isInteger(value) ? String(value) : value.toFixed(digits);
}

function fmtOdds(value: number | null | undefined): string {
  if (value == null) return "—";
  return value > 0 ? `+${value}` : String(value);
}

function fmtPct(value: number | null | undefined): string {
  if (value == null) return "—";
  return `${(value * 100).toFixed(1)}%`;
}

// ─── Tab IDs ──────────────────────────────────────────────────────────────────

type TabId =
  | "outrights"
  | "finishes"
  | "matchups"
  | "three_ball"
  | "player_stats"
  | "recent_form";

const TABS: { id: TabId; label: string }[] = [
  { id: "outrights", label: "Outright Winners" },
  { id: "finishes", label: "Finishes" },
  { id: "matchups", label: "Matchups" },
  { id: "three_ball", label: "3 Ball" },
  { id: "player_stats", label: "Player Stats" },
  { id: "recent_form", label: "Recent Form" },
];

// ─── Column configs ───────────────────────────────────────────────────────────

const OUTRIGHT_COLUMNS: ColumnConfig[] = [
  { key: "player_display_name", label: "Player", width: 150, isNumeric: false },
  {
    key: "american_odds",
    label: "Odds",
    width: 72,
    isNumeric: true,
    formatter: fmtOdds,
  },
  {
    key: "implied_probability",
    label: "Impl%",
    width: 64,
    isNumeric: true,
    formatter: fmtPct,
  },
  {
    key: "expected_round_score",
    label: "Exp Scr",
    width: 72,
    isNumeric: true,
    formatter: (v) => fmt(v, 1),
  },
  {
    key: "sg_total",
    label: "SG Tot",
    width: 64,
    isNumeric: true,
    formatter: (v) => fmt(v, 3),
  },
  {
    key: "sg_approach",
    label: "SG App",
    width: 64,
    isNumeric: true,
    formatter: (v) => fmt(v, 3),
  },
  {
    key: "sg_putting",
    label: "SG Putt",
    width: 64,
    isNumeric: true,
    formatter: (v) => fmt(v, 3),
  },
  {
    key: "course_delta",
    label: "Crs Δ",
    width: 56,
    isNumeric: true,
    formatter: (v) => fmt(v, 2),
  },
];

const PLAYER_STATS_COLUMNS: ColumnConfig[] = [
  { key: "player_name", label: "Player", width: 140, isNumeric: false },
  {
    key: "sg_total",
    label: "SG Tot",
    width: 60,
    isNumeric: true,
    formatter: (v) => fmt(v, 3),
  },
  {
    key: "sg_off_tee",
    label: "SG OT",
    width: 60,
    isNumeric: true,
    formatter: (v) => fmt(v, 3),
  },
  {
    key: "sg_approach",
    label: "SG App",
    width: 60,
    isNumeric: true,
    formatter: (v) => fmt(v, 3),
  },
  {
    key: "sg_putting",
    label: "SG Putt",
    width: 60,
    isNumeric: true,
    formatter: (v) => fmt(v, 3),
  },
  {
    key: "driving_distance",
    label: "DrDist",
    width: 60,
    isNumeric: true,
    formatter: (v) => fmt(v, 1),
  },
  {
    key: "driving_accuracy",
    label: "DrAcc%",
    width: 60,
    isNumeric: true,
    formatter: (v) => fmt(v, 1),
  },
  {
    key: "gir_pct",
    label: "GIR%",
    width: 56,
    isNumeric: true,
    formatter: (v) => fmt(v, 1),
  },
  {
    key: "scrambling_pct",
    label: "Scrmbl%",
    width: 60,
    isNumeric: true,
    formatter: (v) => fmt(v, 1),
  },
  {
    key: "scoring_avg",
    label: "Scoring",
    width: 60,
    isNumeric: true,
    formatter: (v) => fmt(v, 2),
  },
  {
    key: "birdie_avg",
    label: "Birdies",
    width: 60,
    isNumeric: true,
    formatter: (v) => fmt(v, 2),
  },
];

const RECENT_FORM_COLUMNS: ColumnConfig[] = [
  { key: "player_display_name", label: "Player", width: 140, isNumeric: false },
  { key: "tournaments_played", label: "Evts", width: 44, isNumeric: true },
  {
    key: "weighted_l5_score",
    label: "WtdL5",
    width: 64,
    isNumeric: true,
    formatter: (v) => fmt(v, 1),
  },
  {
    key: "l5_finish_avg",
    label: "FinAvgL5",
    width: 70,
    isNumeric: true,
    formatter: (v) => fmt(v, 1),
  },
  {
    key: "l5_to_par_avg",
    label: "ParAvgL5",
    width: 72,
    isNumeric: true,
    formatter: (v) => fmt(v, 1),
  },
  {
    key: "cut_rate_l5",
    label: "Cut%L5",
    width: 64,
    isNumeric: true,
    formatter: fmtPct,
  },
  {
    key: "top10_rate_l5",
    label: "T10%L5",
    width: 64,
    isNumeric: true,
    formatter: fmtPct,
  },
  {
    key: "form_trend_3",
    label: "Trend3",
    width: 60,
    isNumeric: true,
    formatter: (v) => fmt(v, 1),
  },
  {
    key: "days_since_last_event",
    label: "DaysSince",
    width: 76,
    isNumeric: true,
  },
  {
    key: "season_to_par_avg",
    label: "SeasonPar",
    width: 76,
    isNumeric: true,
    formatter: (v) => fmt(v, 1),
  },
];

const FINISHES_COLUMNS: ColumnConfig[] = [
  { key: "player_display_name", label: "Player", width: 140, isNumeric: false },
  {
    key: "american_odds",
    label: "Odds",
    width: 72,
    isNumeric: true,
    formatter: fmtOdds,
  },
  {
    key: "implied_probability",
    label: "Impl%",
    width: 64,
    isNumeric: true,
    formatter: fmtPct,
  },
  {
    key: "model_probability",
    label: "Model%",
    width: 64,
    isNumeric: true,
    formatter: fmtPct,
  },
  {
    key: "betting_edge",
    label: "Edge",
    width: 64,
    isNumeric: true,
    formatter: fmtPct,
  },
  {
    key: "cut_rate_l5",
    label: "Cut%L5",
    width: 64,
    isNumeric: true,
    formatter: fmtPct,
  },
  {
    key: "top10_rate_l5",
    label: "T10%L5",
    width: 64,
    isNumeric: true,
    formatter: fmtPct,
  },
  {
    key: "sg_total",
    label: "SG Tot",
    width: 64,
    isNumeric: true,
    formatter: (v) => fmt(v, 3),
  },
  {
    key: "sg_approach",
    label: "SG App",
    width: 64,
    isNumeric: true,
    formatter: (v) => fmt(v, 3),
  },
  {
    key: "sg_putting",
    label: "SG Putt",
    width: 64,
    isNumeric: true,
    formatter: (v) => fmt(v, 3),
  },
];

// ─── Shared error/loading helpers ─────────────────────────────────────────────

function LoadingView({ colors }: { colors: any }) {
  return (
    <View style={styles.center}>
      <ActivityIndicator color={colors.accent.primary} />
    </View>
  );
}

function ErrorView({
  error,
  refetch,
  colors,
}: {
  error: string;
  refetch: () => void;
  colors: any;
}) {
  return (
    <View style={[styles.center, { gap: 10 }]}>
      <Text style={{ color: colors.text.danger, textAlign: "center" }}>
        {error}
      </Text>
      <Pressable
        onPress={refetch}
        style={[styles.retryBtn, { borderColor: colors.border.subtle }]}
      >
        <Text style={{ color: colors.text.primary, fontWeight: "700" }}>
          Retry
        </Text>
      </Pressable>
    </View>
  );
}

// ─── Tab bar ─────────────────────────────────────────────────────────────────

function TabBar({
  activeTab,
  onSelect,
  colors,
}: {
  activeTab: TabId;
  onSelect: (id: TabId) => void;
  colors: any;
}) {
  return (
    <ScrollView
      horizontal
      showsHorizontalScrollIndicator={false}
      contentContainerStyle={styles.tabBarContent}
    >
      {TABS.map((tab) => {
        const active = tab.id === activeTab;
        return (
          <Pressable
            key={tab.id}
            onPress={() => onSelect(tab.id)}
            style={[
              styles.tabBtn,
              {
                borderColor: active ? "#22D3EE" : colors.border.subtle,
                backgroundColor: active
                  ? "rgba(34,211,238,0.18)"
                  : "rgba(2,6,23,0.35)",
              },
            ]}
          >
            <Text
              style={[
                styles.tabBtnText,
                { color: active ? "#CFFAFE" : colors.text.muted },
              ]}
            >
              {tab.label}
            </Text>
          </Pressable>
        );
      })}
    </ScrollView>
  );
}

// ─── Save-to-betslip button ───────────────────────────────────────────────────

function SaveBtn({
  saved,
  onPress,
}: {
  saved: boolean;
  onPress: () => void;
}) {
  return (
    <Pressable
      onPress={onPress}
      style={[
        styles.saveBtn,
        { backgroundColor: saved ? "rgba(34,211,238,0.25)" : "rgba(34,211,238,0.1)" },
      ]}
    >
      <Text style={[styles.saveBtnText, { color: saved ? "#22D3EE" : "#90B3E9" }]}>
        {saved ? "✓ Saved" : "+ Save"}
      </Text>
    </Pressable>
  );
}

// ─── Outright Winners tab ─────────────────────────────────────────────────────

type OutrightsResponse = {
  tournament_id?: string | null;
  count: number;
  rows: PgaBettingOutrightRow[];
};

function OutrightsTab({ colors }: { colors: any }) {
  const { data, loading, error, refetch } =
    usePgaQuery<OutrightsResponse>("/pga/betting/outrights");
  const { add, remove, items } = usePgaBetslip();
  const { open } = usePgaBetslipDrawer();

  const rows = useMemo(() => data?.rows ?? [], [data]);

  if (loading) return <LoadingView colors={colors} />;
  if (error) return <ErrorView error={error} refetch={refetch} colors={colors} />;

  if (!rows.length) {
    return (
      <View style={styles.center}>
        <Text style={{ color: colors.text.muted }}>No outright odds found.</Text>
      </View>
    );
  }

  return (
    <ScrollView style={styles.flex}>
      <Text style={[styles.tableNote, { color: colors.text.muted, marginBottom: 8 }]}>
        {rows.length} players · tap a column header to sort
      </Text>
      {rows.map((row, idx) => {
        const itemId = `outright-${row.player_id ?? idx}`;
        const saved = items.some((i) => i.id === itemId);
        const oddsStr = fmtOdds(row.american_odds);
        return (
          <View
            key={itemId}
            style={[
              styles.outrightCard,
              { borderColor: colors.border.subtle, backgroundColor: "#0B1529" },
            ]}
          >
            <View style={styles.outrightHeader}>
              <Text style={[styles.outrightName, { color: colors.text.primary }]}>
                {row.player_display_name ?? "—"}
              </Text>
              <View style={styles.outrightOddsRow}>
                <Text style={[styles.outrightOdds, { color: "#CFFAFE" }]}>
                  {oddsStr}
                </Text>
                <Text style={[styles.outrightImpl, { color: colors.text.muted }]}>
                  {fmtPct(row.implied_probability)}
                </Text>
              </View>
            </View>
            <View style={styles.outrightStats}>
              <StatChip label="SG Tot" value={fmt(row.sg_total, 3)} colors={colors} />
              <StatChip label="SG App" value={fmt(row.sg_approach, 3)} colors={colors} />
              <StatChip label="SG Putt" value={fmt(row.sg_putting, 3)} colors={colors} />
              {row.expected_round_score != null && (
                <StatChip label="Exp Scr" value={fmt(row.expected_round_score, 1)} colors={colors} />
              )}
              {row.course_delta != null && (
                <StatChip label="Crs Δ" value={fmt(row.course_delta, 2)} colors={colors} />
              )}
            </View>
            <SaveBtn
              saved={saved}
              onPress={() => {
                if (saved) {
                  remove(itemId);
                } else {
                  add({
                    id: itemId,
                    playerId: row.player_id != null ? String(row.player_id) : null,
                    playerLastName: row.player_display_name?.split(" ").pop() ?? "",
                    playerDisplayName: row.player_display_name ?? "",
                    groupPlayers: [],
                    tournamentId: row.tournament_id ?? undefined,
                    createdAt: new Date().toISOString(),
                    betType: "outright",
                    odds: row.american_odds,
                    description: `${row.player_display_name} to WIN (${oddsStr})`,
                  });
                  open();
                }
              }}
            />
          </View>
        );
      })}
    </ScrollView>
  );
}

// ─── Player Stats tab ─────────────────────────────────────────────────────────

type PlayerStatsResponse = {
  count: number;
  rows: PgaPlayerSkillStatsRow[];
};

function PlayerStatsTab({ colors }: { colors: any }) {
  const { data, loading, error, refetch } =
    usePgaQuery<PlayerStatsResponse>("/pga/betting/player-stats");

  const rows = useMemo(() => data?.rows ?? [], [data]);

  if (loading) return <LoadingView colors={colors} />;
  if (error) return <ErrorView error={error} refetch={refetch} colors={colors} />;

  if (!rows.length) {
    return (
      <View style={styles.center}>
        <Text style={{ color: colors.text.muted }}>No player stats found.</Text>
      </View>
    );
  }

  return (
    <View style={styles.tableWrapper}>
      <Text style={[styles.tableNote, { color: colors.text.muted }]}>
        {rows.length} players · tap a column header to sort
      </Text>
      <AutoSortableTable
        data={rows}
        columns={PLAYER_STATS_COLUMNS}
        defaultSort="sg_total"
      />
    </View>
  );
}

// ─── Recent Form tab ─────────────────────────────────────────────────────────

type RecentFormResponse = {
  count: number;
  rows: PgaRecentFormRow[];
};

function RecentFormTab({ colors }: { colors: any }) {
  const { data, loading, error, refetch } =
    usePgaQuery<RecentFormResponse>("/pga/betting/recent-form");

  const rows = useMemo(() => data?.rows ?? [], [data]);

  if (loading) return <LoadingView colors={colors} />;
  if (error) return <ErrorView error={error} refetch={refetch} colors={colors} />;

  if (!rows.length) {
    return (
      <View style={styles.center}>
        <Text style={{ color: colors.text.muted }}>No form data found.</Text>
      </View>
    );
  }

  return (
    <View style={styles.tableWrapper}>
      <Text style={[styles.tableNote, { color: colors.text.muted }]}>
        {rows.length} players · tap a column header to sort
      </Text>
      <AutoSortableTable
        data={rows}
        columns={RECENT_FORM_COLUMNS}
        defaultSort="weighted_l5_score"
      />
    </View>
  );
}

// ─── Finishes tab ─────────────────────────────────────────────────────────────

type FinishesResponse = {
  tournament_id?: string | null;
  tournament_name?: string | null;
  count: number;
  rows: PgaBettingFinishRow[];
};

const FINISH_MARKET_FILTERS = ["All", "Top 5", "Top 10", "Top 20"];

function FinishesTab({ colors }: { colors: any }) {
  const { data, loading, error, refetch } =
    usePgaQuery<FinishesResponse>("/pga/betting/finishes");
  const { add, remove, items } = usePgaBetslip();
  const { open } = usePgaBetslipDrawer();
  const [marketFilter, setMarketFilter] = useState("All");

  const allRows = useMemo(() => data?.rows ?? [], [data]);

  const markets = useMemo(() => {
    const set = new Set(allRows.map((r) => r.sub_market_name ?? "").filter(Boolean));
    return Array.from(set).sort();
  }, [allRows]);

  const filters = useMemo(
    () => ["All", ...markets],
    [markets]
  );

  const rows = useMemo(
    () =>
      marketFilter === "All"
        ? allRows
        : allRows.filter((r) => r.sub_market_name === marketFilter),
    [allRows, marketFilter]
  );

  if (loading) return <LoadingView colors={colors} />;
  if (error) return <ErrorView error={error} refetch={refetch} colors={colors} />;

  if (!allRows.length) {
    return (
      <View style={styles.center}>
        <Text style={{ color: colors.text.muted }}>No finishes odds found.</Text>
      </View>
    );
  }

  return (
    <View style={styles.flex}>
      {/* Market filter pills */}
      <ScrollView
        horizontal
        showsHorizontalScrollIndicator={false}
        contentContainerStyle={styles.filterBar}
      >
        {filters.map((f) => {
          const active = f === marketFilter;
          return (
            <Pressable
              key={f}
              onPress={() => setMarketFilter(f)}
              style={[
                styles.filterPill,
                {
                  borderColor: active ? "#22D3EE" : colors.border.subtle,
                  backgroundColor: active
                    ? "rgba(34,211,238,0.18)"
                    : "rgba(2,6,23,0.35)",
                },
              ]}
            >
              <Text
                style={[
                  styles.filterPillText,
                  { color: active ? "#CFFAFE" : colors.text.muted },
                ]}
              >
                {f}
              </Text>
            </Pressable>
          );
        })}
      </ScrollView>

      <Text style={[styles.tableNote, { color: colors.text.muted, marginBottom: 8 }]}>
        {rows.length} lines
        {data?.tournament_name ? ` · ${data.tournament_name}` : ""}
      </Text>

      <ScrollView style={styles.flex}>
        {rows.map((row, idx) => {
          const itemId = `finish-${row.player_id}-${row.sub_market_name ?? idx}`;
          const saved = items.some((i) => i.id === itemId);
          const oddsStr = fmtOdds(row.american_odds);
          return (
            <View
              key={itemId}
              style={[
                styles.finishCard,
                { borderColor: colors.border.subtle, backgroundColor: "#0B1529" },
              ]}
            >
              <View style={styles.finishTop}>
                <View style={styles.finishLeft}>
                  {row.sub_market_name ? (
                    <Text style={[styles.finishMarket, { color: "#90B3E9" }]}>
                      {row.sub_market_name}
                    </Text>
                  ) : null}
                  <Text style={[styles.finishName, { color: colors.text.primary }]}>
                    {row.player_display_name ?? "—"}
                  </Text>
                </View>
                <View style={styles.finishRight}>
                  <Text style={[styles.finishOdds, { color: "#CFFAFE" }]}>
                    {oddsStr}
                  </Text>
                  <Text style={[styles.finishImpl, { color: colors.text.muted }]}>
                    {fmtPct(row.implied_probability)} impl
                  </Text>
                  {row.model_probability != null && (
                    <Text style={[styles.finishModel, {
                      color: (row.betting_edge ?? 0) > 0 ? "#4ade80" : colors.text.muted
                    }]}>
                      {fmtPct(row.model_probability)} model
                      {row.betting_edge != null
                        ? ` · Edge: ${fmtPct(row.betting_edge)}`
                        : ""}
                    </Text>
                  )}
                </View>
              </View>
              <View style={styles.finishStats}>
                <StatChip label="SG Tot" value={fmt(row.sg_total, 3)} colors={colors} />
                <StatChip label="SG App" value={fmt(row.sg_approach, 3)} colors={colors} />
                <StatChip label="SG Putt" value={fmt(row.sg_putting, 3)} colors={colors} />
                <StatChip label="Cut%L5" value={fmtPct(row.cut_rate_l5)} colors={colors} />
                <StatChip label="T10%L5" value={fmtPct(row.top10_rate_l5)} colors={colors} />
              </View>
              <SaveBtn
                saved={saved}
                onPress={() => {
                  if (saved) {
                    remove(itemId);
                  } else {
                    add({
                      id: itemId,
                      playerId: row.player_id != null ? String(row.player_id) : null,
                      playerLastName: row.player_display_name?.split(" ").pop() ?? "",
                      playerDisplayName: row.player_display_name ?? "",
                      groupPlayers: [],
                      tournamentId: row.tournament_id ?? undefined,
                      createdAt: new Date().toISOString(),
                      betType: "finish",
                      market: row.sub_market_name,
                      odds: row.american_odds,
                      description: `${row.player_display_name} — ${row.sub_market_name ?? "Finish"} (${oddsStr})`,
                    });
                    open();
                  }
                }}
              />
            </View>
          );
        })}
      </ScrollView>
    </View>
  );
}

// ─── Matchups tab ─────────────────────────────────────────────────────────────

type MatchupsResponse = {
  tournament_id?: string | null;
  count: number;
  rows: PgaBettingMatchupRow[];
};

function MatchupCard({
  row,
  colors,
}: {
  row: PgaBettingMatchupRow;
  colors: any;
}) {
  const [expanded, setExpanded] = useState(false);
  const { add, remove, items } = usePgaBetslip();
  const { open } = usePgaBetslipDrawer();

  const idA = `matchup-a-${row.group_index}-${row.player_a}`;
  const idB = `matchup-b-${row.group_index}-${row.player_b}`;
  const savedA = items.some((i) => i.id === idA);
  const savedB = items.some((i) => i.id === idB);

  function savePlayer(side: "a" | "b") {
    const id = side === "a" ? idA : idB;
    const saved = side === "a" ? savedA : savedB;
    const name = side === "a" ? row.player_a : row.player_b;
    const opponent = side === "a" ? row.player_b : row.player_a;
    const odds = side === "a" ? row.odds_a : row.odds_b;
    const oddsStr = fmtOdds(odds);
    if (saved) {
      remove(id);
    } else {
      add({
        id,
        playerId: null,
        playerLastName: name?.split(" ").pop() ?? "",
        playerDisplayName: name ?? "",
        groupPlayers: [name ?? "", opponent ?? ""].filter(Boolean),
        createdAt: new Date().toISOString(),
        betType: "matchup",
        market: row.sub_market_name,
        odds,
        description: `${name} to beat ${opponent}${row.sub_market_name ? ` (${row.sub_market_name})` : ""} (${oddsStr})`,
      });
      open();
    }
  }

  return (
    <Pressable
      onPress={() => setExpanded((v) => !v)}
      style={[
        styles.matchupCard,
        { borderColor: colors.border.subtle, backgroundColor: "#0B1529" },
      ]}
    >
      {row.sub_market_name ? (
        <Text style={[styles.matchupMarket, { color: "#90B3E9" }]}>
          {row.sub_market_name}
        </Text>
      ) : null}

      {/* Player row */}
      <View style={styles.matchupRow}>
        <View style={styles.matchupSide}>
          <Text style={[styles.matchupName, { color: colors.text.primary }]}>
            {row.player_a ?? "—"}
          </Text>
          <Text style={[styles.matchupOdds, { color: "#CFFAFE" }]}>
            {fmtOdds(row.odds_a)}
          </Text>
          <SaveBtn saved={savedA} onPress={() => savePlayer("a")} />
        </View>

        <Text style={[styles.matchupVs, { color: colors.text.muted }]}>vs</Text>

        <View style={[styles.matchupSide, { alignItems: "flex-end" }]}>
          <Text style={[styles.matchupName, { color: colors.text.primary }]}>
            {row.player_b ?? "—"}
          </Text>
          <Text style={[styles.matchupOdds, { color: "#CFFAFE" }]}>
            {fmtOdds(row.odds_b)}
          </Text>
          <SaveBtn saved={savedB} onPress={() => savePlayer("b")} />
        </View>
      </View>

      {/* Collapsed summary diffs */}
      {!expanded && (
        <View style={styles.matchupDiffs}>
          <Text style={[styles.matchupDiff, { color: colors.text.muted }]}>
            SG Diff: {fmt(row.sg_diff, 3)}
          </Text>
          <Text style={[styles.matchupDiff, { color: colors.text.muted }]}>
            Score Diff: {fmt(row.score_diff, 1)}
          </Text>
          <Text style={[styles.matchupDiff, { color: colors.text.muted }]}>
            App Diff: {fmt(row.approach_diff, 3)}
          </Text>
          <Text style={[styles.matchupDiff, { color: colors.text.muted }]}>
            Putt Diff: {fmt(row.putting_diff, 3)}
          </Text>
        </View>
      )}

      {/* Expanded per-player stats */}
      {expanded && (
        <View style={styles.matchupExpanded}>
          <View style={[styles.matchupExpandedDivider, { backgroundColor: colors.border.subtle }]} />
          <View style={styles.matchupStatsGrid}>
            {/* Left column - Player A */}
            <View style={styles.matchupPlayerStats}>
              <Text style={[styles.matchupPlayerLabel, { color: "#90B3E9" }]}>
                {row.player_a?.split(" ").pop() ?? "A"}
              </Text>
              <StatRow label="Exp Scr" value={fmt(row.score_a, 1)} colors={colors} />
              <StatRow label="SG Total" value={fmt(
                row.sg_diff != null && row.sg_diff > 0 ? row.sg_diff : null, 3
              )} colors={colors} />
              <StatRow label="SG App" value={fmt(
                row.approach_diff != null && row.approach_diff > 0 ? row.approach_diff : null, 3
              )} colors={colors} />
              <StatRow label="SG Putt" value={fmt(
                row.putting_diff != null && row.putting_diff > 0 ? row.putting_diff : null, 3
              )} colors={colors} />
            </View>

            {/* Divider */}
            <View style={[styles.matchupVertDivider, { backgroundColor: colors.border.subtle }]} />

            {/* Right column - Player B */}
            <View style={[styles.matchupPlayerStats, { alignItems: "flex-end" }]}>
              <Text style={[styles.matchupPlayerLabel, { color: "#90B3E9" }]}>
                {row.player_b?.split(" ").pop() ?? "B"}
              </Text>
              <StatRow label="Exp Scr" value={fmt(row.score_b, 1)} colors={colors} right />
              <StatRow label="SG Total" value={fmt(
                row.sg_diff != null && row.sg_diff < 0 ? -row.sg_diff : null, 3
              )} colors={colors} right />
              <StatRow label="SG App" value={fmt(
                row.approach_diff != null && row.approach_diff < 0 ? -row.approach_diff : null, 3
              )} colors={colors} right />
              <StatRow label="SG Putt" value={fmt(
                row.putting_diff != null && row.putting_diff < 0 ? -row.putting_diff : null, 3
              )} colors={colors} right />
            </View>
          </View>

          {/* Raw diffs */}
          <View style={[styles.matchupDivider, { backgroundColor: colors.border.subtle }]} />
          <View style={styles.matchupDiffs}>
            <Text style={[styles.matchupDiff, { color: colors.text.muted }]}>
              SG Diff: {fmt(row.sg_diff, 3)}
            </Text>
            <Text style={[styles.matchupDiff, { color: colors.text.muted }]}>
              Score: {fmt(row.score_diff, 1)}
            </Text>
            <Text style={[styles.matchupDiff, { color: colors.text.muted }]}>
              App: {fmt(row.approach_diff, 3)}
            </Text>
            <Text style={[styles.matchupDiff, { color: colors.text.muted }]}>
              Putt: {fmt(row.putting_diff, 3)}
            </Text>
            {row.course_fit_diff != null && (
              <Text style={[styles.matchupDiff, { color: colors.text.muted }]}>
                Crs: {fmt(row.course_fit_diff, 3)}
              </Text>
            )}
          </View>
          <Text style={[styles.expandHint, { color: colors.text.muted }]}>
            Tap to collapse ▲
          </Text>
        </View>
      )}
      {!expanded && (
        <Text style={[styles.expandHint, { color: colors.text.muted }]}>
          Tap to expand ▼
        </Text>
      )}
    </Pressable>
  );
}

function MatchupsTab({ colors }: { colors: any }) {
  const { data, loading, error, refetch } =
    usePgaQuery<MatchupsResponse>("/pga/betting/matchups");

  const rows = useMemo(() => data?.rows ?? [], [data]);

  if (loading) return <LoadingView colors={colors} />;
  if (error) return <ErrorView error={error} refetch={refetch} colors={colors} />;

  if (!rows.length) {
    return (
      <View style={styles.center}>
        <Text style={{ color: colors.text.muted }}>No matchup odds found.</Text>
      </View>
    );
  }

  return (
    <ScrollView style={styles.flex}>
      <Text style={[styles.tableNote, { color: colors.text.muted, marginBottom: 8 }]}>
        {rows.length} matchups · tap card to expand stats
      </Text>
      {rows.map((row, idx) => (
        <MatchupCard
          key={`${row.group_index}-${idx}`}
          row={row}
          colors={colors}
        />
      ))}
    </ScrollView>
  );
}

// ─── 3 Ball tab ───────────────────────────────────────────────────────────────

type ThreeBallResponse = {
  tournament_id?: string | null;
  count: number;
  rows: PgaBetting3BallRow[];
};

function ThreeBallGroupCard({
  groupIndex,
  players,
  colors,
}: {
  groupIndex: number;
  players: PgaBetting3BallRow[];
  colors: any;
}) {
  const [expanded, setExpanded] = useState(false);
  const { add, remove, items } = usePgaBetslip();
  const { open } = usePgaBetslipDrawer();

  const groupPlayerNames = players.map((p) => p.player_display_name ?? "");

  return (
    <View
      style={[
        styles.threeBallCard,
        { borderColor: colors.border.subtle, backgroundColor: "#0B1529" },
      ]}
    >
      <Text style={[styles.threeBallGroup, { color: "#90B3E9" }]}>
        Group {groupIndex}
      </Text>

      {/* Player comparison grid */}
      <View style={styles.threeBallGrid}>
        {players.map((player, pidx) => {
          const itemId = `3ball-${groupIndex}-${player.player_id ?? pidx}`;
          const saved = items.some((i) => i.id === itemId);
          const oddsStr = fmtOdds(player.american_odds);

          return (
            <View
              key={itemId}
              style={[
                styles.threeBallPlayer,
                pidx < players.length - 1 && {
                  borderRightWidth: StyleSheet.hairlineWidth,
                  borderRightColor: colors.border.subtle,
                },
              ]}
            >
              <Text
                style={[styles.threeBallName, { color: colors.text.primary }]}
                numberOfLines={2}
              >
                {player.player_display_name ?? "—"}
              </Text>
              <Text style={[styles.threeBallOdds, { color: "#CFFAFE" }]}>
                {oddsStr}
              </Text>
              <Text style={[styles.threeBallImpl, { color: colors.text.muted }]}>
                {fmtPct(player.implied_probability)}
              </Text>
              {player.projected_rank != null && (
                <View
                  style={[
                    styles.threeBallRankBadge,
                    {
                      backgroundColor:
                        player.projected_rank === 1
                          ? "rgba(34,211,238,0.2)"
                          : "rgba(255,255,255,0.05)",
                    },
                  ]}
                >
                  <Text
                    style={[
                      styles.threeBallRank,
                      {
                        color:
                          player.projected_rank === 1 ? "#22D3EE" : colors.text.muted,
                      },
                    ]}
                  >
                    Proj #{player.projected_rank}
                  </Text>
                </View>
              )}

              {/* Expanded stats */}
              {expanded && (
                <View style={styles.threeBallExpandedStats}>
                  <Text style={[styles.threeBallStat, { color: colors.text.muted }]}>
                    Exp Scr: {fmt(player.expected_round_score, 1)}
                  </Text>
                </View>
              )}

              <SaveBtn
                saved={saved}
                onPress={() => {
                  if (saved) {
                    remove(itemId);
                  } else {
                    add({
                      id: itemId,
                      playerId: player.player_id != null ? String(player.player_id) : null,
                      playerLastName: player.player_display_name?.split(" ").pop() ?? "",
                      playerDisplayName: player.player_display_name ?? "",
                      groupPlayers: groupPlayerNames,
                      tournamentId: player.tournament_id ?? undefined,
                      createdAt: new Date().toISOString(),
                      betType: "3ball",
                      odds: player.american_odds,
                      description: `${player.player_display_name} to WIN 3-ball Grp ${groupIndex} (${oddsStr})`,
                    });
                    open();
                  }
                }}
              />
            </View>
          );
        })}
      </View>

      {/* Expand toggle */}
      <Pressable
        onPress={() => setExpanded((v) => !v)}
        style={styles.threeBallExpandBtn}
      >
        <Text style={[styles.expandHint, { color: colors.text.muted }]}>
          {expanded ? "Hide stats ▲" : "Show stats ▼"}
        </Text>
      </Pressable>
    </View>
  );
}

function ThreeBallTab({ colors }: { colors: any }) {
  const { data, loading, error, refetch } =
    usePgaQuery<ThreeBallResponse>("/pga/betting/3ball");

  const rows = useMemo(() => data?.rows ?? [], [data]);

  // Group by group_index, preserving order
  const groups = useMemo(() => {
    const map = new Map<number, PgaBetting3BallRow[]>();
    for (const row of rows) {
      const key = row.group_index ?? 0;
      if (!map.has(key)) map.set(key, []);
      map.get(key)!.push(row);
    }
    // Sort players within each group by projected_rank
    for (const [, players] of map) {
      players.sort((a, b) => (a.projected_rank ?? 99) - (b.projected_rank ?? 99));
    }
    return Array.from(map.entries()).sort((a, b) => b[0] - a[0]);
  }, [rows]);

  if (loading) return <LoadingView colors={colors} />;
  if (error) return <ErrorView error={error} refetch={refetch} colors={colors} />;

  if (!rows.length) {
    return (
      <View style={styles.center}>
        <Text style={{ color: colors.text.muted }}>No 3-ball odds found.</Text>
      </View>
    );
  }

  return (
    <ScrollView style={styles.flex}>
      <Text style={[styles.tableNote, { color: colors.text.muted, marginBottom: 8 }]}>
        {groups.length} groups · {rows.length} players
      </Text>
      {groups.map(([groupIndex, players]) => (
        <ThreeBallGroupCard
          key={groupIndex}
          groupIndex={groupIndex}
          players={players}
          colors={colors}
        />
      ))}
    </ScrollView>
  );
}

// ─── Shared stat widgets ───────────────────────────────────────────────────────

function StatChip({
  label,
  value,
  colors,
}: {
  label: string;
  value: string;
  colors: any;
}) {
  return (
    <View style={[styles.statChip, { backgroundColor: "rgba(255,255,255,0.05)" }]}>
      <Text style={[styles.statChipLabel, { color: colors.text.muted }]}>{label}</Text>
      <Text style={[styles.statChipValue, { color: colors.text.primary }]}>{value}</Text>
    </View>
  );
}

function StatRow({
  label,
  value,
  colors,
  right,
}: {
  label: string;
  value: string;
  colors: any;
  right?: boolean;
}) {
  return (
    <View style={[styles.statRow, right && { flexDirection: "row-reverse" }]}>
      <Text style={[styles.statRowLabel, { color: colors.text.muted }]}>{label}</Text>
      <Text style={[styles.statRowValue, { color: colors.text.primary }]}>{value}</Text>
    </View>
  );
}

// ─── Main screen ──────────────────────────────────────────────────────────────

export default function PgaBettingAnalyticsScreen() {
  const { colors } = useTheme();
  const [activeTab, setActiveTab] = useState<TabId>("outrights");
  const { items } = usePgaBetslip();
  const { isOpen, toggle } = usePgaBetslipDrawer();

  function renderActiveTab() {
    switch (activeTab) {
      case "outrights":
        return <OutrightsTab colors={colors} />;
      case "player_stats":
        return <PlayerStatsTab colors={colors} />;
      case "recent_form":
        return <RecentFormTab colors={colors} />;
      case "finishes":
        return <FinishesTab colors={colors} />;
      case "matchups":
        return <MatchupsTab colors={colors} />;
      case "three_ball":
        return <ThreeBallTab colors={colors} />;
    }
  }

  return (
    <>
      <Stack.Screen
        options={{
          title: "Betting Analytics",
          headerStyle: { backgroundColor: colors.surface.screen },
          headerTintColor: colors.text.primary,
        }}
      />
      <View style={[styles.screen, { backgroundColor: colors.surface.screen }]}>
        {/* Header */}
        <View style={[styles.header, { borderColor: colors.border.subtle }]}>
          <Text style={styles.eyebrow}>PGA BETTING ANALYTICS</Text>
          <View style={styles.h1Row}>
            <Text style={[styles.h1, { color: colors.text.primary }]}>
              Bet Board
            </Text>
            {items.length > 0 && (
              <Pressable
                onPress={toggle}
                style={[
                  styles.betslipBadge,
                  { backgroundColor: isOpen ? "rgba(34,211,238,0.25)" : "rgba(34,211,238,0.15)" },
                ]}
              >
                <Text style={styles.betslipBadgeText}>
                  Betslip ({items.length}) {isOpen ? "▼" : "▲"}
                </Text>
              </Pressable>
            )}
          </View>
          <Text style={[styles.sub, { color: colors.text.muted }]}>
            Sortable tables · tap any column header to sort
          </Text>
          <TabBar
            activeTab={activeTab}
            onSelect={setActiveTab}
            colors={colors}
          />
        </View>

        {/* Tab content */}
        <View style={styles.content}>{renderActiveTab()}</View>

        {/* Betslip drawer */}
        <PgaBetslipDrawer />
      </View>
    </>
  );
}

// ─── Styles ───────────────────────────────────────────────────────────────────

const styles = StyleSheet.create({
  screen: { flex: 1 },
  flex: { flex: 1 },
  header: {
    borderBottomWidth: StyleSheet.hairlineWidth,
    backgroundColor: "#071731",
    paddingHorizontal: 16,
    paddingTop: 14,
    paddingBottom: 0,
  },
  eyebrow: {
    color: "#90B3E9",
    fontSize: 11,
    fontWeight: "700",
    letterSpacing: 1,
  },
  h1Row: {
    flexDirection: "row",
    alignItems: "center",
    gap: 10,
    marginTop: 4,
  },
  h1: {
    fontSize: 22,
    fontWeight: "800",
  },
  betslipBadge: {
    borderRadius: 999,
    paddingHorizontal: 10,
    paddingVertical: 4,
  },
  betslipBadgeText: {
    color: "#22D3EE",
    fontSize: 11,
    fontWeight: "800",
  },
  sub: {
    fontSize: 12,
    marginTop: 2,
    marginBottom: 10,
  },
  tabBarContent: {
    gap: 8,
    paddingBottom: 12,
    paddingRight: 12,
  },
  tabBtn: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 999,
    paddingHorizontal: 12,
    paddingVertical: 6,
    minHeight: 30,
    justifyContent: "center",
  },
  tabBtnText: {
    fontSize: 11,
    fontWeight: "700",
  },
  content: {
    flex: 1,
    padding: 12,
  },
  tableWrapper: {
    flex: 1,
    gap: 8,
  },
  tableNote: {
    fontSize: 11,
  },
  center: {
    flex: 1,
    justifyContent: "center",
    alignItems: "center",
  },
  retryBtn: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 10,
    paddingHorizontal: 14,
    paddingVertical: 8,
  },
  // Save button
  saveBtn: {
    alignSelf: "flex-start",
    borderRadius: 6,
    paddingHorizontal: 8,
    paddingVertical: 4,
    marginTop: 6,
  },
  saveBtnText: {
    fontSize: 11,
    fontWeight: "700",
  },
  // Stat chips
  statChip: {
    borderRadius: 6,
    paddingHorizontal: 6,
    paddingVertical: 4,
    alignItems: "center",
  },
  statChipLabel: {
    fontSize: 9,
    fontWeight: "600",
    letterSpacing: 0.3,
  },
  statChipValue: {
    fontSize: 12,
    fontWeight: "700",
  },
  statRow: {
    flexDirection: "row",
    gap: 6,
    alignItems: "center",
    marginBottom: 2,
  },
  statRowLabel: {
    fontSize: 10,
    width: 52,
  },
  statRowValue: {
    fontSize: 11,
    fontWeight: "700",
  },
  // Outright cards
  outrightCard: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 12,
    padding: 12,
    marginBottom: 8,
    gap: 8,
  },
  outrightHeader: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "flex-start",
  },
  outrightName: {
    fontSize: 15,
    fontWeight: "700",
    flex: 1,
  },
  outrightOddsRow: {
    alignItems: "flex-end",
    gap: 2,
  },
  outrightOdds: {
    fontSize: 18,
    fontWeight: "800",
  },
  outrightImpl: {
    fontSize: 11,
  },
  outrightStats: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 6,
  },
  // Filter bar
  filterBar: {
    gap: 8,
    paddingBottom: 8,
    paddingTop: 2,
  },
  filterPill: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 999,
    paddingHorizontal: 12,
    paddingVertical: 5,
  },
  filterPillText: {
    fontSize: 11,
    fontWeight: "700",
  },
  // Finish cards
  finishCard: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 12,
    padding: 12,
    marginBottom: 8,
    gap: 8,
  },
  finishTop: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "flex-start",
  },
  finishLeft: {
    flex: 1,
    gap: 2,
  },
  finishRight: {
    alignItems: "flex-end",
    gap: 2,
  },
  finishMarket: {
    fontSize: 10,
    fontWeight: "700",
    letterSpacing: 0.5,
    textTransform: "uppercase",
  },
  finishName: {
    fontSize: 14,
    fontWeight: "700",
  },
  finishOdds: {
    fontSize: 18,
    fontWeight: "800",
  },
  finishImpl: {
    fontSize: 11,
  },
  finishModel: {
    fontSize: 11,
    fontWeight: "600",
  },
  finishStats: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 6,
  },
  // Matchup cards
  matchupCard: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 12,
    padding: 12,
    marginBottom: 8,
    gap: 6,
  },
  matchupMarket: {
    fontSize: 10,
    fontWeight: "700",
    letterSpacing: 0.5,
    textTransform: "uppercase",
  },
  matchupRow: {
    flexDirection: "row",
    alignItems: "flex-start",
    justifyContent: "space-between",
    gap: 8,
  },
  matchupSide: {
    flex: 1,
    gap: 2,
  },
  matchupName: {
    fontSize: 13,
    fontWeight: "700",
  },
  matchupOdds: {
    fontSize: 16,
    fontWeight: "800",
  },
  matchupVs: {
    fontSize: 11,
    fontWeight: "600",
    paddingTop: 4,
  },
  matchupDiffs: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 8,
    marginTop: 4,
  },
  matchupDiff: {
    fontSize: 10,
  },
  matchupExpanded: {
    gap: 8,
  },
  matchupExpandedDivider: {
    height: StyleSheet.hairlineWidth,
    marginVertical: 4,
  },
  matchupStatsGrid: {
    flexDirection: "row",
    gap: 8,
  },
  matchupPlayerStats: {
    flex: 1,
    gap: 4,
  },
  matchupPlayerLabel: {
    fontSize: 11,
    fontWeight: "800",
    marginBottom: 4,
  },
  matchupVertDivider: {
    width: StyleSheet.hairlineWidth,
  },
  matchupDivider: {
    height: StyleSheet.hairlineWidth,
  },
  expandHint: {
    fontSize: 10,
    textAlign: "center",
    marginTop: 4,
  },
  // 3-ball cards
  threeBallCard: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 12,
    padding: 12,
    marginBottom: 8,
    gap: 8,
  },
  threeBallGroup: {
    fontSize: 10,
    fontWeight: "700",
    letterSpacing: 0.5,
    textTransform: "uppercase",
  },
  threeBallGrid: {
    flexDirection: "row",
    gap: 0,
  },
  threeBallPlayer: {
    flex: 1,
    paddingHorizontal: 6,
    gap: 3,
    alignItems: "center",
  },
  threeBallName: {
    fontSize: 12,
    fontWeight: "700",
    textAlign: "center",
  },
  threeBallOdds: {
    fontSize: 16,
    fontWeight: "800",
    textAlign: "center",
  },
  threeBallImpl: {
    fontSize: 11,
    textAlign: "center",
  },
  threeBallRankBadge: {
    borderRadius: 4,
    paddingHorizontal: 6,
    paddingVertical: 2,
  },
  threeBallRank: {
    fontSize: 10,
    fontWeight: "700",
  },
  threeBallExpandedStats: {
    gap: 2,
    alignItems: "center",
  },
  threeBallStat: {
    fontSize: 10,
    textAlign: "center",
  },
  threeBallExpandBtn: {
    paddingTop: 4,
  },
});

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
  { key: "tournaments_played", label: "Evts", width: 44, isNumeric: true },
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
    key: "weighted_l5_score",
    label: "WtdL5",
    width: 64,
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
  { key: "sub_market_name", label: "Market", width: 80, isNumeric: false },
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

const THREE_BALL_COLUMNS: ColumnConfig[] = [
  { key: "group_index", label: "Grp", width: 44, isNumeric: true },
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
    key: "expected_round_score",
    label: "Exp Score",
    width: 80,
    isNumeric: true,
    formatter: (v) => fmt(v, 1),
  },
  { key: "projected_rank", label: "Proj Rank", width: 80, isNumeric: true },
];

// ─── Placeholder tab ──────────────────────────────────────────────────────────

function ComingSoonTab({
  label,
  colors,
}: {
  label: string;
  colors: any;
}) {
  return (
    <View style={[styles.comingSoon, { borderColor: colors.border.subtle }]}>
      <Text style={[styles.comingSoonTitle, { color: colors.text.primary }]}>
        {label}
      </Text>
      <Text style={[styles.comingSoonSub, { color: colors.text.muted }]}>
        Coming soon
      </Text>
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

// ─── Outright Winners tab ─────────────────────────────────────────────────────

type OutrightsResponse = {
  tournament_id?: string | null;
  tournament_name?: string | null;
  count: number;
  rows: PgaBettingOutrightRow[];
};

function OutrightsTab({ colors }: { colors: any }) {
  const { data, loading, error, refetch } =
    usePgaQuery<OutrightsResponse>("/pga/betting/outrights");

  const rows = useMemo(() => data?.rows ?? [], [data]);

  if (loading) {
    return (
      <View style={styles.center}>
        <ActivityIndicator color={colors.accent.primary} />
      </View>
    );
  }

  if (error) {
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

  if (!rows.length) {
    return (
      <View style={styles.center}>
        <Text style={{ color: colors.text.muted }}>No outright odds found.</Text>
      </View>
    );
  }

  return (
    <View style={styles.tableWrapper}>
      {data?.tournament_name ? (
        <Text style={[styles.tableNote, { color: colors.text.muted }]}>
          {data.tournament_name} · {rows.length} players
        </Text>
      ) : null}
      <AutoSortableTable
        data={rows}
        columns={OUTRIGHT_COLUMNS}
        defaultSort="american_odds"
      />
    </View>
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

  if (loading) {
    return (
      <View style={styles.center}>
        <ActivityIndicator color={colors.accent.primary} />
      </View>
    );
  }

  if (error) {
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

  if (loading) {
    return (
      <View style={styles.center}>
        <ActivityIndicator color={colors.accent.primary} />
      </View>
    );
  }

  if (error) {
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

// ─── Finishes tab ────────────────────────────────────────────────────────────

type FinishesResponse = {
  tournament_id?: string | null;
  tournament_name?: string | null;
  count: number;
  rows: PgaBettingFinishRow[];
};

function FinishesTab({ colors }: { colors: any }) {
  const { data, loading, error, refetch } =
    usePgaQuery<FinishesResponse>("/pga/betting/finishes");

  const rows = useMemo(() => data?.rows ?? [], [data]);

  if (loading) {
    return (
      <View style={styles.center}>
        <ActivityIndicator color={colors.accent.primary} />
      </View>
    );
  }

  if (error) {
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

  if (!rows.length) {
    return (
      <View style={styles.center}>
        <Text style={{ color: colors.text.muted }}>No finishes odds found.</Text>
      </View>
    );
  }

  return (
    <View style={styles.tableWrapper}>
      {data?.tournament_name ? (
        <Text style={[styles.tableNote, { color: colors.text.muted }]}>
          {data.tournament_name} · {rows.length} lines
        </Text>
      ) : null}
      <AutoSortableTable
        data={rows}
        columns={FINISHES_COLUMNS}
        defaultSort="american_odds"
      />
    </View>
  );
}

// ─── Matchups tab ────────────────────────────────────────────────────────────

type MatchupsResponse = {
  tournament_id?: string | null;
  count: number;
  rows: PgaBettingMatchupRow[];
};

function MatchupsTab({ colors }: { colors: any }) {
  const { data, loading, error, refetch } =
    usePgaQuery<MatchupsResponse>("/pga/betting/matchups");

  const rows = useMemo(() => data?.rows ?? [], [data]);

  if (loading) {
    return (
      <View style={styles.center}>
        <ActivityIndicator color={colors.accent.primary} />
      </View>
    );
  }

  if (error) {
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

  if (!rows.length) {
    return (
      <View style={styles.center}>
        <Text style={{ color: colors.text.muted }}>No matchup odds found.</Text>
      </View>
    );
  }

  return (
    <ScrollView style={styles.matchupsList}>
      {data?.rows ? (
        <Text style={[styles.tableNote, { color: colors.text.muted, padding: 4 }]}>
          {rows.length} matchups · tap row to expand
        </Text>
      ) : null}
      {rows.map((row, idx) => (
        <View
          key={`${row.group_index}-${idx}`}
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
          <View style={styles.matchupRow}>
            <View style={styles.matchupPlayer}>
              <Text style={[styles.matchupName, { color: colors.text.primary }]}>
                {row.player_a ?? "—"}
              </Text>
              <Text style={[styles.matchupOdds, { color: "#CFFAFE" }]}>
                {fmtOdds(row.odds_a)}
              </Text>
            </View>
            <Text style={[styles.matchupVs, { color: colors.text.muted }]}>vs</Text>
            <View style={[styles.matchupPlayer, { alignItems: "flex-end" }]}>
              <Text style={[styles.matchupName, { color: colors.text.primary }]}>
                {row.player_b ?? "—"}
              </Text>
              <Text style={[styles.matchupOdds, { color: "#CFFAFE" }]}>
                {fmtOdds(row.odds_b)}
              </Text>
            </View>
          </View>
          <View style={styles.matchupStats}>
            <Text style={[styles.matchupStat, { color: colors.text.muted }]}>
              SG Diff: {fmt(row.sg_diff, 3)}
            </Text>
            <Text style={[styles.matchupStat, { color: colors.text.muted }]}>
              Score Diff: {fmt(row.score_diff, 1)}
            </Text>
            <Text style={[styles.matchupStat, { color: colors.text.muted }]}>
              App Diff: {fmt(row.approach_diff, 3)}
            </Text>
            <Text style={[styles.matchupStat, { color: colors.text.muted }]}>
              Putt Diff: {fmt(row.putting_diff, 3)}
            </Text>
          </View>
        </View>
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

function ThreeBallTab({ colors }: { colors: any }) {
  const { data, loading, error, refetch } =
    usePgaQuery<ThreeBallResponse>("/pga/betting/3ball");

  const rows = useMemo(() => data?.rows ?? [], [data]);

  if (loading) {
    return (
      <View style={styles.center}>
        <ActivityIndicator color={colors.accent.primary} />
      </View>
    );
  }

  if (error) {
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

  if (!rows.length) {
    return (
      <View style={styles.center}>
        <Text style={{ color: colors.text.muted }}>No 3-ball odds found.</Text>
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
        columns={THREE_BALL_COLUMNS}
        defaultSort="group_index"
      />
    </View>
  );
}

// ─── Main screen ──────────────────────────────────────────────────────────────

export default function PgaBettingAnalyticsScreen() {
  const { colors } = useTheme();
  const [activeTab, setActiveTab] = useState<TabId>("outrights");

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
        <View
          style={[styles.header, { borderColor: colors.border.subtle }]}
        >
          <Text style={styles.eyebrow}>PGA BETTING ANALYTICS</Text>
          <Text style={[styles.h1, { color: colors.text.primary }]}>
            Bet Board
          </Text>
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
      </View>
    </>
  );
}

// ─── Styles ───────────────────────────────────────────────────────────────────

const styles = StyleSheet.create({
  screen: {
    flex: 1,
  },
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
  h1: {
    fontSize: 22,
    fontWeight: "800",
    marginTop: 4,
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
  comingSoon: {
    flex: 1,
    justifyContent: "center",
    alignItems: "center",
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 16,
    backgroundColor: "#0B1529",
    gap: 8,
  },
  comingSoonTitle: {
    fontSize: 18,
    fontWeight: "800",
  },
  comingSoonSub: {
    fontSize: 13,
  },
  matchupsList: {
    flex: 1,
  },
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
    alignItems: "center",
    justifyContent: "space-between",
    gap: 8,
  },
  matchupPlayer: {
    flex: 1,
    gap: 2,
  },
  matchupName: {
    fontSize: 13,
    fontWeight: "700",
  },
  matchupOdds: {
    fontSize: 14,
    fontWeight: "800",
  },
  matchupVs: {
    fontSize: 11,
    fontWeight: "600",
  },
  matchupStats: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 8,
    marginTop: 4,
  },
  matchupStat: {
    fontSize: 10,
  },
});

import { useMemo, useState } from "react";
import {
  ActivityIndicator,
  FlatList,
  Linking,
  Pressable,
  ScrollView,
  StyleSheet,
  Text,
  View,
} from "react-native";
import * as Clipboard from "expo-clipboard";

import {
  EplBettingAnalyticsRow,
  useEplBettingAnalytics,
} from "@/hooks/epl/useEplBettingAnalytics";
import { useEplQuery } from "@/hooks/epl/useEplQuery";
import { useTheme } from "@/store/useTheme";
import { useSoccerBetslip } from "@/store/useSoccerBetslip";
import { useSoccerBetslipDrawer } from "@/store/useSoccerBetslipDrawer";

const GAMBLY_URL = "https://www.gambly.com/gambly-bot";

// ─── Helpers ──────────────────────────────────────────────────────────────────

function formatPct(value?: number | null) {
  if (value == null) return "–";
  return `${(value * 100).toFixed(1)}%`;
}

function formatPrice(value?: number | null) {
  if (value == null) return "–";
  return value > 0 ? `+${value}` : `${value}`;
}

function formatLine(value?: number | null) {
  return value == null ? "" : ` (${value})`;
}

function formatMetric(value?: number | null, digits = 2) {
  if (value == null) return "–";
  return Number.isInteger(value) ? `${value}` : value.toFixed(digits);
}

function toSlipText(row: EplBettingAnalyticsRow) {
  return `${row.game} — ${row.market}: ${row.outcome}${formatLine(row.line)} @ ${formatPrice(row.price)} (${row.bookmaker})`;
}

function parseTeams(game: string): [string, string] {
  const parts = game.split(" vs ");
  return [parts[0]?.trim() ?? "", parts[1]?.trim() ?? ""];
}

// Pretty labels for known market keys
const MARKET_LABEL: Record<string, string> = {
  all: "All",
  h2h: "1X2",
  totals: "O/U",
  btts: "BTTS",
  player_cards: "Cards",
  spreads: "Spread",
  draw_no_bet: "DNB",
};

function getMarketLabel(market: string) {
  return MARKET_LABEL[market.toLowerCase()] ?? market;
}

// H2H outcome sort order
const H2H_ORDER = ["Home", "Draw", "Away"];

// ─── Types ─────────────────────────────────────────────────────────────────────

type TeamMasterRow = {
  team_name: string;
  last3_avg_scored?: number | null;
  last3_avg_allowed?: number | null;
  last5_avg_scored?: number | null;
  last5_avg_allowed?: number | null;
  l3_team_cards_pg?: number | null;
  l3_total_cards_pg?: number | null;
  season_avg_goals_scored?: number | null;
  season_avg_goals_allowed?: number | null;
};

type GameGroup = {
  game: string;
  homeTeam: string;
  awayTeam: string;
  startTime?: string;
  rows: EplBettingAnalyticsRow[];
};

// ─── Team lookup (fuzzy) ───────────────────────────────────────────────────────

function buildTeamMap(rows: TeamMasterRow[]): Record<string, TeamMasterRow> {
  const map: Record<string, TeamMasterRow> = {};
  for (const row of rows) {
    if (row.team_name) map[row.team_name.toLowerCase().trim()] = row;
  }
  return map;
}

function findTeam(
  map: Record<string, TeamMasterRow>,
  name: string
): TeamMasterRow | undefined {
  const key = name.toLowerCase().trim();
  if (map[key]) return map[key];
  for (const [k, v] of Object.entries(map)) {
    if (k.includes(key) || key.includes(k)) return v;
  }
  return undefined;
}

// ─── Sub-components ────────────────────────────────────────────────────────────

function EdgeBadge({ tier }: { tier?: string | null }) {
  if (!tier) return null;
  const bg =
    tier === "Strong"
      ? "rgba(74,222,128,0.20)"
      : tier === "Medium"
        ? "rgba(251,191,36,0.20)"
        : "rgba(148,163,184,0.15)";
  const fg =
    tier === "Strong" ? "#86EFAC" : tier === "Medium" ? "#FDE68A" : "#94A3B8";
  return (
    <View style={[styles.edgeBadge, { backgroundColor: bg }]}>
      <Text style={[styles.edgeBadgeText, { color: fg }]}>{tier}</Text>
    </View>
  );
}

function OddsRow({
  rows,
  market,
  colors,
  onSave,
  onCopy,
}: {
  rows: EplBettingAnalyticsRow[];
  market: string;
  colors: any;
  onSave: (row: EplBettingAnalyticsRow) => void;
  onCopy: (row: EplBettingAnalyticsRow) => void;
}) {
  const isH2H = market.toLowerCase() === "h2h";
  const sorted = isH2H
    ? [...rows].sort(
        (a, b) =>
          (H2H_ORDER.indexOf(a.outcome) === -1
            ? 99
            : H2H_ORDER.indexOf(a.outcome)) -
          (H2H_ORDER.indexOf(b.outcome) === -1
            ? 99
            : H2H_ORDER.indexOf(b.outcome))
      )
    : rows;

  if (isH2H) {
    return (
      <View style={styles.h2hRow}>
        {sorted.map((row) => (
          <Pressable
            key={row.outcome}
            style={[styles.h2hCell, { borderColor: colors.border.subtle }]}
            onPress={() => onSave(row)}
          >
            <Text style={[styles.h2hLabel, { color: colors.text.muted }]}>
              {row.outcome}
            </Text>
            <Text style={styles.h2hPrice}>{formatPrice(row.price)}</Text>
            <Text style={[styles.h2hImplied, { color: colors.text.muted }]}>
              {formatPct(row.implied_probability)}
            </Text>
          </Pressable>
        ))}
      </View>
    );
  }

  return (
    <View style={styles.outcomeList}>
      {sorted.map((row, idx) => (
        <Pressable
          key={idx}
          style={[styles.outcomeRow, { borderColor: colors.border.subtle }]}
          onPress={() => onSave(row)}
        >
          <View style={{ flex: 1 }}>
            <Text style={[styles.outcomeName, { color: colors.text.primary }]}>
              {row.outcome}
              {row.line != null ? ` (${row.line})` : ""}
            </Text>
            <Text style={[styles.outcomeBook, { color: colors.text.muted }]}>
              {row.bookmaker}
            </Text>
          </View>
          <View style={styles.outcomePriceCol}>
            <Text style={styles.outcomePrice}>{formatPrice(row.price)}</Text>
            <Text style={[styles.outcomeImplied, { color: colors.text.muted }]}>
              {formatPct(row.implied_probability)}
            </Text>
          </View>
        </Pressable>
      ))}
    </View>
  );
}

function StatRow({
  label,
  homeVal,
  awayVal,
  colors,
  highlightBetter,
}: {
  label: string;
  homeVal: string;
  awayVal: string;
  colors: any;
  highlightBetter?: "higher" | "lower";
}) {
  const homeNum = parseFloat(homeVal);
  const awayNum = parseFloat(awayVal);
  const homeWins =
    highlightBetter === "higher"
      ? homeNum > awayNum
      : highlightBetter === "lower"
        ? homeNum < awayNum
        : false;
  const awayWins =
    highlightBetter === "higher"
      ? awayNum > homeNum
      : highlightBetter === "lower"
        ? awayNum < homeNum
        : false;

  return (
    <View style={styles.statRow}>
      <Text style={[styles.statVal, homeWins ? styles.statHighlight : { color: colors.text.primary }]}>
        {homeVal}
      </Text>
      <Text style={[styles.statLabel, { color: colors.text.muted }]}>{label}</Text>
      <Text style={[styles.statVal, { textAlign: "right" }, awayWins ? styles.statHighlight : { color: colors.text.primary }]}>
        {awayVal}
      </Text>
    </View>
  );
}

function GameCard({
  group,
  selectedMarket,
  teamMap,
  colors,
  addToBetslip,
  openDrawer,
}: {
  group: GameGroup;
  selectedMarket: string;
  teamMap: Record<string, TeamMasterRow>;
  colors: any;
  addToBetslip: (item: any) => void;
  openDrawer: () => void;
}) {
  const { game, homeTeam, awayTeam, startTime, rows } = group;

  // Rows for the selected market (client-side filter)
  const marketRows = useMemo(() => {
    if (selectedMarket === "all") return rows;
    return rows.filter(
      (r) => r.market.toLowerCase() === selectedMarket.toLowerCase()
    );
  }, [rows, selectedMarket]);

  const homeStats = findTeam(teamMap, homeTeam);
  const awayStats = findTeam(teamMap, awayTeam);

  // Model stats — first row with model data
  const modelRow = rows.find((r) => r.model_expected_total_goals != null || r.model_edge_tier != null);
  const isCardsMarket = selectedMarket.toLowerCase() === "player_cards";

  function handleSave(row: EplBettingAnalyticsRow) {
    addToBetslip({
      id: `${row.game}-${row.market}-${row.outcome}-${row.line ?? "na"}`,
      league: row.league,
      game: row.game,
      start_time_et: row.start_time_et,
      market: row.market,
      outcome: row.outcome,
      line: row.line,
      price: row.price,
      bookmaker: row.bookmaker,
      rationale: `Edge vs market: ${formatPct(row.probability_vs_market)} | Tier: ${row.model_edge_tier ?? "–"}`,
    });
  }

  async function handleCopy(row: EplBettingAnalyticsRow) {
    await Clipboard.setStringAsync(toSlipText(row));
    openDrawer();
  }

  return (
    <View style={[styles.card, { borderColor: colors.border.subtle }]}>
      {/* ── Game header ── */}
      <View style={styles.cardHeader}>
        <View style={{ flex: 1 }}>
          <View style={styles.teamRow}>
            <Text style={[styles.teamName, { color: colors.text.primary }]}>
              {homeTeam}
            </Text>
            <Text style={[styles.vsText, { color: colors.text.muted }]}>vs</Text>
            <Text style={[styles.teamName, { color: colors.text.primary }]}>
              {awayTeam}
            </Text>
          </View>
          {startTime ? (
            <Text style={[styles.kickoffText, { color: colors.text.muted }]}>
              {startTime}
            </Text>
          ) : null}
        </View>
        {modelRow?.model_edge_tier ? (
          <EdgeBadge tier={modelRow.model_edge_tier} />
        ) : null}
      </View>

      {/* ── Team stats ── */}
      {(homeStats || awayStats) ? (
        <View style={[styles.statsBox, { borderColor: colors.border.subtle }]}>
          {/* Column labels */}
          <View style={styles.statRow}>
            <Text style={[styles.statColHeader, { color: "#22D3EE" }]}>
              {homeTeam.split(" ").pop()}
            </Text>
            <Text style={[styles.statColHeaderCenter, { color: colors.text.muted }]} />
            <Text style={[styles.statColHeader, { color: "#A78BFA", textAlign: "right" }]}>
              {awayTeam.split(" ").pop()}
            </Text>
          </View>

          <StatRow
            label="L3 Goals"
            homeVal={formatMetric(homeStats?.last3_avg_scored)}
            awayVal={formatMetric(awayStats?.last3_avg_scored)}
            colors={colors}
            highlightBetter="higher"
          />
          <StatRow
            label="L3 Goals Allowed"
            homeVal={formatMetric(homeStats?.last3_avg_allowed)}
            awayVal={formatMetric(awayStats?.last3_avg_allowed)}
            colors={colors}
            highlightBetter="lower"
          />
          <StatRow
            label="L5 Goals"
            homeVal={formatMetric(homeStats?.last5_avg_scored)}
            awayVal={formatMetric(awayStats?.last5_avg_scored)}
            colors={colors}
            highlightBetter="higher"
          />
          <StatRow
            label="L5 Goals Allowed"
            homeVal={formatMetric(homeStats?.last5_avg_allowed)}
            awayVal={formatMetric(awayStats?.last5_avg_allowed)}
            colors={colors}
            highlightBetter="lower"
          />
          {isCardsMarket ? (
            <StatRow
              label="L3 Cards/Gm"
              homeVal={formatMetric(homeStats?.l3_team_cards_pg)}
              awayVal={formatMetric(awayStats?.l3_team_cards_pg)}
              colors={colors}
            />
          ) : null}
        </View>
      ) : null}

      {/* ── Model stats strip ── */}
      {modelRow ? (
        <View style={styles.modelStrip}>
          {modelRow.model_expected_total_goals != null && (
            <View style={styles.modelStat}>
              <Text style={[styles.modelStatVal, { color: colors.text.primary }]}>
                {formatMetric(modelRow.model_expected_total_goals)}
              </Text>
              <Text style={[styles.modelStatLabel, { color: colors.text.muted }]}>
                xGoals
              </Text>
            </View>
          )}
          {modelRow.model_total_line_edge != null && (
            <View style={styles.modelStat}>
              <Text style={[styles.modelStatVal, { color: colors.text.primary }]}>
                {formatMetric(modelRow.model_total_line_edge)}
              </Text>
              <Text style={[styles.modelStatLabel, { color: colors.text.muted }]}>
                Line Edge
              </Text>
            </View>
          )}
          {modelRow.probability_vs_market != null && (
            <View style={styles.modelStat}>
              <Text style={[styles.modelStatVal, { color: "#4ADE80" }]}>
                {formatPct(modelRow.probability_vs_market)}
              </Text>
              <Text style={[styles.modelStatLabel, { color: colors.text.muted }]}>
                vs Mkt
              </Text>
            </View>
          )}
        </View>
      ) : null}

      {/* ── Divider ── */}
      <View style={[styles.divider, { backgroundColor: colors.border.subtle }]} />

      {/* ── Market odds ── */}
      {marketRows.length > 0 ? (
        <OddsRow
          rows={marketRows}
          market={selectedMarket === "all" ? marketRows[0]?.market ?? "" : selectedMarket}
          colors={colors}
          onSave={handleSave}
          onCopy={handleCopy}
        />
      ) : (
        <Text style={[styles.noOddsText, { color: colors.text.muted }]}>
          No odds for this market
        </Text>
      )}

      {/* ── Actions ── */}
      <View style={styles.actionsRow}>
        <Pressable
          style={[styles.secondaryBtn, { borderColor: colors.border.subtle }]}
          onPress={() =>
            marketRows[0] ? handleCopy(marketRows[0]) : openDrawer()
          }
        >
          <Text style={[styles.secondaryBtnText, { color: colors.text.primary }]}>
            Copy for Gambly
          </Text>
        </Pressable>
        <Pressable
          style={[styles.secondaryBtn, { borderColor: colors.border.subtle }]}
          onPress={() => Linking.openURL(GAMBLY_URL)}
        >
          <Text style={[styles.secondaryBtnText, { color: colors.text.primary }]}>
            Open Gambly
          </Text>
        </Pressable>
      </View>
    </View>
  );
}

// ─── Main screen ───────────────────────────────────────────────────────────────

export default function EplBettingAnalyticsScreen() {
  const { colors } = useTheme();
  const addToBetslip = useSoccerBetslip((s) => s.add);
  const openDrawer = useSoccerBetslipDrawer((s) => s.open);

  const [selectedMarket, setSelectedMarket] = useState<string>("all");
  const [onlyBestPrice, setOnlyBestPrice] = useState(true);

  const { data, loading, error, refetch } = useEplBettingAnalytics({
    only_best_price: onlyBestPrice,
    limit: 500,
  });

  const { data: rawTeamData } = useEplQuery<TeamMasterRow[]>(
    "/epl/team-master-metrics"
  );

  const teamMap = useMemo(
    () => buildTeamMap(rawTeamData ?? []),
    [rawTeamData]
  );

  const markets = useMemo(
    () => ["all", ...(data?.available_markets ?? [])],
    [data?.available_markets]
  );

  // Group rows by game
  const gameGroups = useMemo<GameGroup[]>(() => {
    const rows = data?.rows ?? [];
    const map = new Map<string, EplBettingAnalyticsRow[]>();
    for (const row of rows) {
      if (!map.has(row.game)) map.set(row.game, []);
      map.get(row.game)!.push(row);
    }
    return [...map.entries()].map(([game, gameRows]) => {
      const [homeTeam, awayTeam] = parseTeams(game);
      return { game, homeTeam, awayTeam, startTime: gameRows[0]?.start_time_et, rows: gameRows };
    });
  }, [data?.rows]);

  // Filter to games that have rows for the selected market (unless "all")
  const visibleGroups = useMemo<GameGroup[]>(() => {
    if (selectedMarket === "all") return gameGroups;
    return gameGroups.filter((g) =>
      g.rows.some(
        (r) => r.market.toLowerCase() === selectedMarket.toLowerCase()
      )
    );
  }, [gameGroups, selectedMarket]);

  if (loading) {
    return (
      <View style={[styles.center, { backgroundColor: "#050A18" }]}>
        <ActivityIndicator color={colors.accent.primary} />
      </View>
    );
  }

  if (error) {
    return (
      <View style={[styles.center, { backgroundColor: "#050A18", padding: 16, gap: 10 }]}>
        <Text style={{ color: "#fff", textAlign: "center" }}>
          Failed to load EPL betting analytics: {error}
        </Text>
        <Pressable
          onPress={refetch}
          style={[styles.retryBtn, { borderColor: colors.border.subtle }]}
        >
          <Text style={{ color: colors.text.primary, fontWeight: "700" }}>Retry</Text>
        </Pressable>
      </View>
    );
  }

  return (
    <FlatList
      style={{ flex: 1, backgroundColor: "#050A18" }}
      contentContainerStyle={{ padding: 12, paddingBottom: 40, gap: 12 }}
      data={visibleGroups}
      keyExtractor={(item) => item.game}
      ListHeaderComponent={
        <View style={[styles.header, { borderColor: colors.border.subtle }]}>
          <Text style={styles.eyebrow}>EPL BETTING ANALYTICS</Text>
          <Text style={styles.h1}>Live Bet Board</Text>
          <Text style={[styles.sub, { color: colors.text.muted }]}>
            {data?.date_et ?? "–"} · {visibleGroups.length} matches
          </Text>

          {/* Market selector */}
          <Text style={[styles.filterLabel, { color: colors.text.muted }]}>Market</Text>
          <ScrollView
            horizontal
            showsHorizontalScrollIndicator={false}
            contentContainerStyle={styles.pillRow}
          >
            {markets.map((market) => {
              const active = selectedMarket === market;
              return (
                <Pressable
                  key={market}
                  onPress={() => setSelectedMarket(market)}
                  style={[
                    styles.pill,
                    {
                      borderColor: active ? "#22D3EE" : colors.border.subtle,
                      backgroundColor: active
                        ? "rgba(34,211,238,0.22)"
                        : "transparent",
                    },
                  ]}
                >
                  <Text
                    style={[
                      styles.pillText,
                      { color: active ? "#CFFAFE" : colors.text.muted },
                    ]}
                  >
                    {getMarketLabel(market)}
                  </Text>
                </Pressable>
              );
            })}
          </ScrollView>

          {/* Best price toggle */}
          <Pressable
            style={[styles.toggleBtn, { borderColor: colors.border.subtle }]}
            onPress={() => setOnlyBestPrice((v) => !v)}
          >
            <View
              style={[
                styles.toggleDot,
                { backgroundColor: onlyBestPrice ? "#22D3EE" : colors.text.muted },
              ]}
            />
            <Text style={[styles.toggleLabel, { color: colors.text.primary }]}>
              {onlyBestPrice ? "Best price only" : "All prices"}
            </Text>
          </Pressable>
        </View>
      }
      renderItem={({ item }) => (
        <GameCard
          group={item}
          selectedMarket={selectedMarket}
          teamMap={teamMap}
          colors={colors}
          addToBetslip={addToBetslip}
          openDrawer={openDrawer}
        />
      )}
      ListEmptyComponent={
        <View style={[styles.emptyCard, { borderColor: colors.border.subtle }]}>
          <Text style={[styles.emptyText, { color: colors.text.primary }]}>
            No matches found
          </Text>
          <Text style={[styles.sub, { color: colors.text.muted }]}>
            Try a different market filter or disable best price only.
          </Text>
        </View>
      }
    />
  );
}

// ─── Styles ────────────────────────────────────────────────────────────────────

const styles = StyleSheet.create({
  center: { flex: 1, justifyContent: "center", alignItems: "center" },
  retryBtn: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 10,
    paddingHorizontal: 12,
    paddingVertical: 8,
  },

  // Header
  header: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 16,
    backgroundColor: "#071731",
    padding: 16,
  },
  eyebrow: { color: "#90B3E9", fontSize: 11, fontWeight: "700", letterSpacing: 1 },
  h1: { color: "#E9F2FF", fontSize: 22, fontWeight: "800", marginTop: 6 },
  sub: { marginTop: 4, fontSize: 12 },
  filterLabel: { fontSize: 11, fontWeight: "700", marginTop: 14 },
  pillRow: { gap: 8, paddingTop: 8, paddingRight: 12 },
  pill: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 999,
    paddingHorizontal: 14,
    paddingVertical: 7,
  },
  pillText: { fontSize: 11, fontWeight: "700" },
  toggleBtn: {
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
    marginTop: 12,
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 10,
    paddingHorizontal: 12,
    paddingVertical: 8,
  },
  toggleDot: { width: 8, height: 8, borderRadius: 4 },
  toggleLabel: { fontSize: 12, fontWeight: "700" },

  // Game card
  card: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 16,
    backgroundColor: "#0B1529",
    padding: 14,
    gap: 10,
  },

  // Card header
  cardHeader: {
    flexDirection: "row",
    alignItems: "flex-start",
    justifyContent: "space-between",
    gap: 8,
  },
  teamRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
    flexWrap: "wrap",
  },
  teamName: { fontWeight: "800", fontSize: 15 },
  vsText: { fontSize: 12, fontWeight: "600" },
  kickoffText: { fontSize: 11, marginTop: 3 },
  edgeBadge: { borderRadius: 999, paddingHorizontal: 10, paddingVertical: 4 },
  edgeBadgeText: { fontSize: 10, fontWeight: "800" },

  // Stats
  statsBox: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 12,
    padding: 10,
    gap: 5,
    backgroundColor: "rgba(148,163,184,0.06)",
  },
  statRow: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
  },
  statColHeader: { fontSize: 11, fontWeight: "800", flex: 1 },
  statColHeaderCenter: { flex: 1, textAlign: "center" },
  statLabel: { fontSize: 11, flex: 1.6, textAlign: "center" },
  statVal: { fontSize: 12, fontWeight: "700", flex: 1 },
  statHighlight: { color: "#4ADE80" },

  // Model strip
  modelStrip: {
    flexDirection: "row",
    gap: 16,
    paddingHorizontal: 4,
  },
  modelStat: { alignItems: "center", gap: 2 },
  modelStatVal: { fontSize: 13, fontWeight: "800" },
  modelStatLabel: { fontSize: 10, fontWeight: "600" },

  // Divider
  divider: { height: StyleSheet.hairlineWidth },

  // H2H odds
  h2hRow: { flexDirection: "row", gap: 8 },
  h2hCell: {
    flex: 1,
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 10,
    padding: 10,
    alignItems: "center",
    gap: 3,
    backgroundColor: "rgba(148,163,184,0.06)",
  },
  h2hLabel: { fontSize: 10, fontWeight: "700" },
  h2hPrice: { color: "#86EFAC", fontSize: 15, fontWeight: "800" },
  h2hImplied: { fontSize: 10 },

  // Outcome list (non-H2H)
  outcomeList: { gap: 6 },
  outcomeRow: {
    flexDirection: "row",
    alignItems: "center",
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 10,
    padding: 10,
    backgroundColor: "rgba(148,163,184,0.06)",
  },
  outcomeName: { fontSize: 13, fontWeight: "700" },
  outcomeBook: { fontSize: 10, marginTop: 2 },
  outcomePriceCol: { alignItems: "flex-end", gap: 2 },
  outcomePrice: { color: "#86EFAC", fontSize: 15, fontWeight: "800" },
  outcomeImplied: { fontSize: 10 },

  noOddsText: { fontSize: 12, textAlign: "center", paddingVertical: 8 },

  // Actions
  actionsRow: { flexDirection: "row", gap: 8 },
  secondaryBtn: {
    flex: 1,
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 10,
    paddingVertical: 9,
    alignItems: "center",
  },
  secondaryBtnText: { fontWeight: "700", fontSize: 12 },

  // Empty
  emptyCard: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 14,
    backgroundColor: "#0B1529",
    padding: 20,
    alignItems: "center",
    gap: 6,
  },
  emptyText: { fontWeight: "700", fontSize: 14 },
});

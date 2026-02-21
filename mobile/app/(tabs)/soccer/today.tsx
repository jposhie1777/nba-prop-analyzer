import { useMemo, useState } from "react";
import { ActivityIndicator, FlatList, Pressable, ScrollView, StyleSheet, Text, View } from "react-native";

import { useEplQuery } from "@/hooks/epl/useEplQuery";
import { useTheme } from "@/store/useTheme";
import { useSoccerBetslip } from "@/store/useSoccerBetslip";
import { useSoccerBetslipDrawer } from "@/store/useSoccerBetslipDrawer";

type SoccerMarket = {
  league: string;
  game: string;
  start_time_et?: string;
  market: string;
  outcome: string;
  line?: number | null;
  best_price: number;
  best_bookmaker: string;
  implied_prob: number;
  model_confidence: number;
  edge: number;
  rationale?: string;
  recommended?: boolean;
  home_season_gf?: number;
  home_season_ga?: number;
  home_l10_gf?: number;
  home_l10_ga?: number;
  home_season_cards?: number;
  away_season_gf?: number;
  away_season_ga?: number;
  away_l10_gf?: number;
  away_l10_ga?: number;
  away_season_cards?: number;
  combined_season_gf?: number;
  combined_season_ga?: number;
  combined_season_cards?: number;
};

type SoccerResponse = {
  date_et: string;
  slate_size: number;
  markets_count: number;
  suggestions: SoccerMarket[];
  all_markets: SoccerMarket[];
};

type NormalizedMarket = "alternate_totals" | "btts" | "double_chance" | "draw_no_bet" | "outright_winner" | "other";

type MarketGroup = {
  key: NormalizedMarket;
  label: string;
  bets: { market: SoccerMarket; index: number }[];
};

type BetSection = {
  key: "goal_bets" | "match_bets" | "other_bets";
  label: string;
  groups: MarketGroup[];
};

type TotalsSide = "over" | "under";
type LeagueFilter = "EPL" | "La Liga" | "MLS";
type LeagueView = "bets" | "analytics";

const LEAGUE_TABS: LeagueFilter[] = ["EPL", "La Liga", "MLS"];

const ANALYTICS_FIELDS: { key: keyof SoccerMarket; label: string }[] = [
  { key: "home_season_gf", label: "Home avg goals scored" },
  { key: "home_season_ga", label: "Home avg goals against" },
  { key: "home_l10_gf", label: "Home L10 goals scored" },
  { key: "home_l10_ga", label: "Home L10 goals against" },
  { key: "home_season_cards", label: "Home avg cards" },
  { key: "away_season_gf", label: "Away avg goals scored" },
  { key: "away_season_ga", label: "Away avg goals against" },
  { key: "away_l10_gf", label: "Away L10 goals scored" },
  { key: "away_l10_ga", label: "Away L10 goals against" },
  { key: "away_season_cards", label: "Away avg cards" },
  { key: "combined_season_gf", label: "Combined avg goals scored" },
  { key: "combined_season_ga", label: "Combined avg goals against" },
  { key: "combined_season_cards", label: "Combined avg cards" },
];

function formatPct(value?: number) {
  if (value == null) return "-";
  return `${(value * 100).toFixed(1)}%`;
}

function formatLine(line?: number | null) {
  if (line == null) return "";
  return ` (${line})`;
}

function formatPrice(price: number) {
  return price > 0 ? `+${price}` : `${price}`;
}

function getTotalsSide(outcome: string): TotalsSide | null {
  const normalized = outcome.trim().toLowerCase();
  if (normalized.startsWith("over")) return "over";
  if (normalized.startsWith("under")) return "under";
  return null;
}

function isIntegerOrHalf(line?: number | null) {
  if (line == null) return false;
  const rounded = Math.round(line * 2) / 2;
  return Math.abs(line - rounded) < 1e-6;
}

function compareByLineAscending(a: SoccerMarket, b: SoccerMarket) {
  const aLine = a.line ?? Number.POSITIVE_INFINITY;
  const bLine = b.line ?? Number.POSITIVE_INFINITY;
  return aLine - bLine;
}

function normalizeMarket(market: string): NormalizedMarket {
  const normalized = market.trim().toLowerCase().replace(/-/g, "_").replace(/\s+/g, "_");
  if (["h2h", "moneyline", "match_winner", "winner", "outright_winner"].includes(normalized)) {
    return "outright_winner";
  }
  if (["alternate_totals", "alt_totals", "total_goals", "totals", "over_under"].includes(normalized)) {
    return "alternate_totals";
  }
  if (["btts", "both_teams_to_score"].includes(normalized)) return "btts";
  if (["double_chance"].includes(normalized)) return "double_chance";
  if (["draw_no_bet", "dnb"].includes(normalized)) return "draw_no_bet";
  return "other";
}

function displayMarket(market: string) {
  const normalized = normalizeMarket(market);
  if (normalized === "alternate_totals") return "Total Goals";
  if (normalized === "btts") return "Both Teams to Score";
  if (normalized === "double_chance") return "Double Chance";
  if (normalized === "draw_no_bet") return "Draw No Bet";
  if (normalized === "outright_winner") return "Outright Winner";
  return market;
}

function normalizeLeague(league: string): LeagueFilter | null {
  const normalized = league.trim().toLowerCase().replace(/\s+/g, "");
  if (normalized === "epl") return "EPL";
  if (["laliga", "la_liga"].includes(normalized)) return "La Liga";
  if (normalized === "mls") return "MLS";
  return null;
}

function formatMetric(value?: number) {
  if (value == null) return "-";
  return Number.isInteger(value) ? `${value}` : value.toFixed(2);
}

export default function SoccerTodayScreen() {
  const { colors } = useTheme();
  const addToBetslip = useSoccerBetslip((s) => s.add);
  const openDrawer = useSoccerBetslipDrawer((s) => s.open);
  const { data, loading, error, refetch } = useEplQuery<SoccerResponse>("/soccer/todays-betting-analysis");
  const [selectedLeague, setSelectedLeague] = useState<LeagueFilter>("EPL");
  const [selectedView, setSelectedView] = useState<LeagueView>("bets");
  const [selectedByGame, setSelectedByGame] = useState<Record<string, number>>({});
  const [totalsSideByGame, setTotalsSideByGame] = useState<Record<string, TotalsSide>>({});

  const games = useMemo(() => {
    const grouped = new Map<string, SoccerMarket[]>();
    const orderedGames: string[] = [];
    const candidates = data?.all_markets?.length ? data.all_markets : data?.suggestions ?? [];

    for (const market of candidates) {
      if (!grouped.has(market.game)) {
        grouped.set(market.game, []);
        orderedGames.push(market.game);
      }
      grouped.get(market.game)?.push(market);
    }

    return orderedGames.map((game) => {
      const markets = grouped.get(game) ?? [];
      const defaultBet = markets.find((m) => m.recommended) ?? markets[0];

      const goalBets: MarketGroup[] = [];
      const matchBets: MarketGroup[] = [];
      const otherBets: MarketGroup[] = [];

      const byMarket = new Map<NormalizedMarket, { market: SoccerMarket; index: number }[]>();
      for (const [index, market] of markets.entries()) {
        const normalized = normalizeMarket(market.market);
        if (!byMarket.has(normalized)) {
          byMarket.set(normalized, []);
        }
        byMarket.get(normalized)?.push({ market, index });
      }

      byMarket.forEach((bets, key) => {
        const group: MarketGroup = {
          key,
          label: displayMarket(bets[0]?.market.market ?? key),
          bets,
        };
        if (["alternate_totals", "btts"].includes(key)) {
          goalBets.push(group);
        } else if (["double_chance", "draw_no_bet", "outright_winner"].includes(key)) {
          matchBets.push(group);
        } else {
          otherBets.push(group);
        }
      });

      const sections: BetSection[] = [];
      if (goalBets.length) sections.push({ key: "goal_bets", label: "Goal Bets", groups: goalBets });
      if (matchBets.length) sections.push({ key: "match_bets", label: "Match Bets", groups: matchBets });
      if (otherBets.length) sections.push({ key: "other_bets", label: "Other Bets", groups: otherBets });

      return { game, markets, defaultBet, sections };
    });
  }, [data?.all_markets, data?.suggestions]);

  const filteredGames = useMemo(
    () => games.filter((game) => normalizeLeague(game.markets[0]?.league ?? "") === selectedLeague),
    [games, selectedLeague]
  );

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
        <Text style={{ color: "#fff", textAlign: "center" }}>Failed to load soccer bets: {error}</Text>
        <Pressable onPress={refetch} style={[styles.retryBtn, { borderColor: colors.border.subtle }]}> 
          <Text style={{ color: colors.text.primary, fontWeight: "700" }}>Retry</Text>
        </Pressable>
      </View>
    );
  }

  return (
    <FlatList
      style={{ flex: 1, backgroundColor: "#050A18" }}
      contentContainerStyle={{ padding: 12, paddingBottom: 30, gap: 10 }}
      ListHeaderComponent={
        <View style={[styles.hero, { borderColor: colors.border.subtle }]}> 
          <Text style={styles.eyebrow}>SOCCER BETTING ANALYSIS</Text>
          <Text style={styles.h1}>Today&apos;s Suggested Bets</Text>
          <Text style={[styles.sub, { color: colors.text.muted }]}> 
            Date: {data?.date_et ?? "-"} • Games: {data?.slate_size ?? 0} • Markets: {data?.markets_count ?? 0}
          </Text>

          <View style={styles.tabRow}>
            {LEAGUE_TABS.map((league) => {
              const isSelected = selectedLeague === league;
              return (
                <Pressable
                  key={league}
                  onPress={() => setSelectedLeague(league)}
                  style={[
                    styles.tabPill,
                    {
                      borderColor: isSelected ? "#3B82F6" : colors.border.subtle,
                      backgroundColor: isSelected ? "rgba(59,130,246,0.22)" : "transparent",
                    },
                  ]}
                >
                  <Text style={[styles.tabPillText, { color: isSelected ? "#DBEAFE" : colors.text.muted }]}>{league}</Text>
                </Pressable>
              );
            })}
          </View>

          <View style={styles.tabRow}>
            {[
              { key: "bets" as const, label: "Bet Board" },
              { key: "analytics" as const, label: "Team Analytics" },
            ].map((view) => {
              const isSelected = selectedView === view.key;
              return (
                <Pressable
                  key={view.key}
                  onPress={() => setSelectedView(view.key)}
                  style={[
                    styles.tabPill,
                    {
                      borderColor: isSelected ? "#22D3EE" : colors.border.subtle,
                      backgroundColor: isSelected ? "rgba(34,211,238,0.22)" : "transparent",
                    },
                  ]}
                >
                  <Text style={[styles.tabPillText, { color: isSelected ? "#CFFAFE" : colors.text.muted }]}>{view.label}</Text>
                </Pressable>
              );
            })}
          </View>
        </View>
      }
      data={filteredGames}
      keyExtractor={(item) => item.game}
      renderItem={({ item }) => {
        const totalsSide = totalsSideByGame[item.game] ?? "over";
        const totalsCandidates = item.markets
          .filter((market) => normalizeMarket(market.market) === "alternate_totals")
          .filter((market) => isIntegerOrHalf(market.line))
          .filter((market) => getTotalsSide(market.outcome) === totalsSide)
          .sort(compareByLineAscending);

        const selectedIndex = selectedByGame[item.game];
        const selectedByIndex = selectedIndex == null ? undefined : item.markets[selectedIndex];

        const selectedBet =
          selectedByIndex ??
          totalsCandidates[0] ??
          item.defaultBet ??
          item.markets[0];

        if (!selectedBet) {
          return null;
        }

        const betId = `${selectedBet.game}-${selectedBet.market}-${selectedBet.outcome}-${selectedBet.line ?? "n/a"}`;

        if (selectedView === "analytics") {
          return (
            <View style={[styles.card, { borderColor: colors.border.subtle }]}> 
              <Text style={[styles.game, { color: colors.text.primary }]}>{item.game}</Text>
              <Text style={[styles.meta, { color: colors.text.muted }]}>{selectedBet.league}</Text>

              <View style={[styles.analyticsBox, { borderColor: colors.border.subtle }]}> 
                <Text style={[styles.analyticsTitle, { color: colors.text.primary }]}>Team + Match Analytics</Text>
                {ANALYTICS_FIELDS.map((field) => (
                  <View key={`${item.game}-${field.key}`} style={styles.analyticsRow}>
                    <Text style={[styles.analyticsKey, { color: colors.text.muted }]}>{field.label}</Text>
                    <Text style={[styles.analyticsValue, { color: colors.text.primary }]}>{formatMetric(selectedBet[field.key] as number | undefined)}</Text>
                  </View>
                ))}
              </View>
            </View>
          );
        }

        return (
          <View style={[styles.card, { borderColor: colors.border.subtle }]}> 
            <View style={styles.cardTopRow}>
              <Text style={[styles.game, { color: colors.text.primary }]}>{item.game}</Text>
              <View style={styles.pricePill}>
                <Text style={styles.priceText}>{formatPrice(selectedBet.best_price)}</Text>
              </View>
            </View>

            <View style={styles.sectionWrap}>
              {item.sections.map((section) => (
                <View key={section.key} style={styles.sectionBlock}>
                  <Text style={[styles.sectionTitle, { color: colors.text.primary }]}>{section.label}</Text>

                  {section.groups.map((group) => (
                    <View key={group.key} style={styles.marketGroup}>
                      <Text style={[styles.marketGroupTitle, { color: colors.text.muted }]}>{group.label}</Text>
                      <ScrollView
                        horizontal
                        showsHorizontalScrollIndicator={false}
                        contentContainerStyle={styles.betPillScrollRow}
                      >
                        {(group.key === "alternate_totals"
                          ? group.bets
                              .filter(({ market }) => isIntegerOrHalf(market.line))
                              .filter(({ market }) => getTotalsSide(market.outcome) === (totalsSideByGame[item.game] ?? "over"))
                              .sort((a, b) => compareByLineAscending(a.market, b.market))
                          : group.bets
                        ).map(({ market, index }, marketIndex) => {
                          const selected = index === selectedByGame[item.game] || (selectedByGame[item.game] == null && market === selectedBet);
                          return (
                            <Pressable
                              key={`${group.key}-${market.outcome}-${market.line ?? marketIndex}`}
                              onPress={() =>
                                setSelectedByGame((prev) => ({
                                  ...prev,
                                  [item.game]: index,
                                }))
                              }
                              style={[
                                styles.betPill,
                                {
                                  borderColor: selected ? "#3B82F6" : colors.border.subtle,
                                  backgroundColor: selected ? "rgba(59,130,246,0.22)" : "transparent",
                                },
                              ]}
                            >
                              <Text style={[styles.betPillText, { color: selected ? "#DBEAFE" : colors.text.muted }]}>
                                {market.outcome}
                                {formatLine(market.line)}
                              </Text>
                            </Pressable>
                          );
                        })}
                      </ScrollView>

                      {group.key === "alternate_totals" && (
                        <View style={styles.toggleRow}>
                          {(["over", "under"] as TotalsSide[]).map((side) => {
                            const isSelected = (totalsSideByGame[item.game] ?? "over") === side;
                            return (
                              <Pressable
                                key={`${item.game}-${side}`}
                                onPress={() => {
                                  setTotalsSideByGame((prev) => ({ ...prev, [item.game]: side }));

                                  const nextPick = group.bets
                                    .filter(({ market }) => isIntegerOrHalf(market.line))
                                    .filter(({ market }) => getTotalsSide(market.outcome) === side)
                                    .sort((a, b) => compareByLineAscending(a.market, b.market))[0];

                                  if (nextPick) {
                                    setSelectedByGame((prev) => ({
                                      ...prev,
                                      [item.game]: nextPick.index,
                                    }));
                                  }
                                }}
                                style={[
                                  styles.toggleBtn,
                                  {
                                    borderColor: isSelected ? "#3B82F6" : colors.border.subtle,
                                    backgroundColor: isSelected ? "rgba(59,130,246,0.18)" : "transparent",
                                  },
                                ]}
                              >
                                <Text style={[styles.toggleText, { color: isSelected ? "#DBEAFE" : colors.text.muted }]}>
                                  {side === "over" ? "Over" : "Under"}
                                </Text>
                              </Pressable>
                            );
                          })}
                        </View>
                      )}
                    </View>
                  ))}
                </View>
              ))}
            </View>

            <Text style={[styles.market, { color: colors.text.primary }]}> 
              {displayMarket(selectedBet.market)}: {selectedBet.outcome}
              {formatLine(selectedBet.line)}
            </Text>

            <Text style={[styles.meta, { color: colors.text.muted }]}> 
              {selectedBet.league} • {selectedBet.best_bookmaker}
            </Text>

            <View style={[styles.analyticsBox, { borderColor: colors.border.subtle }]}> 
              <Text style={[styles.analyticsTitle, { color: colors.text.primary }]}>Bet Analytics</Text>
              <View style={styles.metricsRow}>
                <Text style={[styles.metric, { color: colors.text.muted }]}>Implied {formatPct(selectedBet.implied_prob)}</Text>
                <Text style={[styles.metric, { color: colors.text.muted }]}>Model {formatPct(selectedBet.model_confidence)}</Text>
                <Text style={[styles.metric, { color: "#4ADE80" }]}>Edge {formatPct(selectedBet.edge)}</Text>
                <Text style={[styles.metric, { color: colors.text.muted }]}>Odds {formatPrice(selectedBet.best_price)}</Text>
              </View>

              {!!selectedBet.rationale && (
                <Text style={[styles.rationale, { color: colors.text.muted }]}>{selectedBet.rationale}</Text>
              )}
            </View>

            <View style={styles.actionsRow}>
              <Pressable
                style={[styles.primaryBtn, { backgroundColor: "#1E40AF" }]}
                onPress={() => {
                  addToBetslip({
                    id: betId,
                    league: selectedBet.league,
                    game: selectedBet.game,
                    start_time_et: selectedBet.start_time_et,
                    market: selectedBet.market,
                    outcome: selectedBet.outcome,
                    line: selectedBet.line,
                    price: selectedBet.best_price,
                    bookmaker: selectedBet.best_bookmaker,
                    rationale: selectedBet.rationale,
                  });
                }}
              >
                <Text style={styles.primaryBtnText}>Save Bet</Text>
              </Pressable>

              <Pressable style={[styles.secondaryBtn, { borderColor: colors.border.subtle }]} onPress={openDrawer}>
                <Text style={[styles.secondaryBtnText, { color: colors.text.primary }]}>Send to Gambly</Text>
              </Pressable>
            </View>
          </View>
        );
      }}
      ListEmptyComponent={
        <View style={[styles.card, { borderColor: colors.border.subtle }]}> 
          <Text style={[styles.market, { color: colors.text.primary }]}>No {selectedLeague} results found for this view.</Text>
          <Text style={[styles.meta, { color: colors.text.muted }]}>Try another league or refresh once today&apos;s markets are ingested.</Text>
        </View>
      }
    />
  );
}

const styles = StyleSheet.create({
  center: { flex: 1, justifyContent: "center", alignItems: "center" },
  retryBtn: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 10,
    paddingHorizontal: 12,
    paddingVertical: 8,
  },
  hero: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 16,
    backgroundColor: "#071731",
    padding: 16,
    marginBottom: 8,
  },
  eyebrow: { color: "#90B3E9", fontSize: 11, fontWeight: "700" },
  h1: { color: "#E9F2FF", fontSize: 22, fontWeight: "800", marginTop: 8 },
  sub: { marginTop: 8, fontSize: 12 },
  tabRow: { flexDirection: "row", gap: 8, marginTop: 10, flexWrap: "wrap" },
  tabPill: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 999,
    paddingHorizontal: 12,
    paddingVertical: 7,
  },
  tabPillText: { fontSize: 11, fontWeight: "800" },
  card: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 14,
    backgroundColor: "#0B1529",
    padding: 12,
    gap: 6,
  },
  cardTopRow: { flexDirection: "row", alignItems: "center", justifyContent: "space-between" },
  game: { fontWeight: "700", fontSize: 14, flex: 1, paddingRight: 8 },
  pricePill: { backgroundColor: "rgba(74,222,128,0.2)", borderRadius: 999, paddingHorizontal: 10, paddingVertical: 4 },
  priceText: { color: "#86EFAC", fontWeight: "800", fontSize: 12 },
  market: { fontWeight: "700", fontSize: 13 },
  meta: { fontSize: 12 },
  metricsRow: { flexDirection: "row", flexWrap: "wrap", gap: 10, marginTop: 2 },
  sectionWrap: { marginTop: 4, gap: 10 },
  sectionBlock: { gap: 6 },
  sectionTitle: { fontSize: 11, fontWeight: "800", letterSpacing: 0.4, textTransform: "uppercase" },
  marketGroup: { gap: 6 },
  marketGroupTitle: { fontSize: 11, fontWeight: "700" },
  toggleRow: { flexDirection: "row", gap: 8 },
  toggleBtn: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 999,
    paddingHorizontal: 10,
    paddingVertical: 6,
  },
  toggleText: { fontSize: 11, fontWeight: "700" },
  betPillScrollRow: { flexDirection: "row", gap: 8, paddingRight: 8 },
  betPill: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 999,
    paddingHorizontal: 10,
    paddingVertical: 6,
  },
  betPillText: { fontSize: 11, fontWeight: "700" },
  analyticsBox: {
    marginTop: 6,
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 10,
    padding: 10,
    gap: 4,
    backgroundColor: "rgba(148, 163, 184, 0.08)",
  },
  analyticsTitle: { fontSize: 12, fontWeight: "700" },
  analyticsRow: { flexDirection: "row", justifyContent: "space-between", gap: 8 },
  analyticsKey: { fontSize: 12 },
  analyticsValue: { fontSize: 12, fontWeight: "700" },
  metric: { fontSize: 12, fontWeight: "600" },
  rationale: { fontSize: 12, lineHeight: 16, marginTop: 2 },
  actionsRow: { flexDirection: "row", gap: 8, marginTop: 8 },
  primaryBtn: { borderRadius: 10, paddingVertical: 10, alignItems: "center", flex: 1 },
  primaryBtnText: { color: "#DBEAFE", fontWeight: "800", fontSize: 12 },
  secondaryBtn: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 10,
    paddingVertical: 10,
    alignItems: "center",
    flex: 1,
  },
  secondaryBtnText: { fontWeight: "700", fontSize: 12 },
});

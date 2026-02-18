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

export default function SoccerTodayScreen() {
  const { colors } = useTheme();
  const addToBetslip = useSoccerBetslip((s) => s.add);
  const openDrawer = useSoccerBetslipDrawer((s) => s.open);
  const { data, loading, error, refetch } = useEplQuery<SoccerResponse>("/soccer/todays-betting-analysis");
  const [selectedByGame, setSelectedByGame] = useState<Record<string, number>>({});

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
        </View>
      }
      data={games}
      keyExtractor={(item) => item.game}
      renderItem={({ item }) => {
        const selectedBet =
          item.markets[selectedByGame[item.game]] ??
          item.defaultBet ??
          item.markets[0];

        if (!selectedBet) {
          return null;
        }

        const betId = `${selectedBet.game}-${selectedBet.market}-${selectedBet.outcome}-${selectedBet.line ?? "n/a"}`;

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
                        {group.bets.map(({ market, index }, marketIndex) => {
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
          <Text style={[styles.market, { color: colors.text.primary }]}>No recommended bets found yet.</Text>
          <Text style={[styles.meta, { color: colors.text.muted }]}>Try lowering the edge threshold on the API if needed.</Text>
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

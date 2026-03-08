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
  implied_prob?: number;
  model_confidence?: number;
  edge?: number;
  rationale?: string;
  recommended?: boolean;
  home_season_gf?: number;
  home_season_ga?: number;
  home_l10_gf?: number;
  home_l10_ga?: number;
  away_season_gf?: number;
  away_season_ga?: number;
  away_l10_gf?: number;
  away_l10_ga?: number;
  home_l3_goals_scored?: number;
  home_l3_goals_allowed?: number;
  home_l3_win_rate?: number;
  home_l5_goals_scored?: number;
  home_l5_goals_allowed?: number;
  home_l5_win_rate?: number;
  home_l7_goals_scored?: number;
  home_l7_goals_allowed?: number;
  home_l7_win_rate?: number;
  away_l3_goals_scored?: number;
  away_l3_goals_allowed?: number;
  away_l3_win_rate?: number;
  away_l5_goals_scored?: number;
  away_l5_goals_allowed?: number;
  away_l5_win_rate?: number;
  away_l7_goals_scored?: number;
  away_l7_goals_allowed?: number;
  away_l7_win_rate?: number;
};

type SoccerResponse = {
  date_et: string;
  slate_size: number;
  markets_count: number;
  all_markets: SoccerMarket[];
};

type NormalizedMarket = "alternate_totals" | "btts" | "double_chance" | "draw_no_bet" | "outright_winner" | "other";
type LeagueFilter = "EPL" | "La Liga" | "MLS";
type AnalyticsWindow = "l3" | "l5" | "l7" | "season" | "current_season";

const LEAGUE_TABS: LeagueFilter[] = ["EPL", "La Liga", "MLS"];
const WINDOW_TABS: AnalyticsWindow[] = ["l3", "l5", "l7", "season", "current_season"];

function normalizeMarket(market: string): NormalizedMarket {
  const normalized = market.trim().toLowerCase().replace(/-/g, "_").replace(/\s+/g, "_");
  if (["h2h", "moneyline", "match_winner", "winner", "outright_winner", "spreads", "spread"].includes(normalized)) {
    return "outright_winner";
  }
  if (["alternate_totals", "alt_totals", "total_goals", "totals", "over_under", "team_totals"].includes(normalized)) {
    return "alternate_totals";
  }
  if (["btts", "both_teams_to_score"].includes(normalized)) return "btts";
  if (normalized === "double_chance") return "double_chance";
  if (["draw_no_bet", "dnb"].includes(normalized)) return "draw_no_bet";
  return "other";
}

function displayMarket(market: NormalizedMarket) {
  if (market === "alternate_totals") return "Total Goals";
  if (market === "btts") return "BTTS";
  if (market === "double_chance") return "Double Chance";
  if (market === "draw_no_bet") return "Draw No Bet";
  if (market === "outright_winner") return "Moneyline";
  return "Other";
}

function hasAnalytics(market: SoccerMarket) {
  return [
    market.home_l3_goals_scored,
    market.home_l5_goals_scored,
    market.home_l7_goals_scored,
    market.away_l3_goals_scored,
    market.away_l5_goals_scored,
    market.away_l7_goals_scored,
    market.home_season_gf,
    market.away_season_gf,
  ].some((value) => value != null);
}

function isGoalTotalWithinRange(market: SoccerMarket) {
  if (market.line == null) return false;
  return market.line <= 4.5;
}

function normalizeLeague(league: string): LeagueFilter | null {
  const normalized = league.trim().toLowerCase().replace(/\s+/g, "");
  if (normalized === "epl") return "EPL";
  if (["laliga", "la_liga"].includes(normalized)) return "La Liga";
  if (normalized === "mls") return "MLS";
  return null;
}

function formatPrice(price: number) {
  return price > 0 ? `+${price}` : `${price}`;
}

function formatPercent(value?: number) {
  if (value == null) return "—";
  return `${Math.round(value * 100)}%`;
}

function formatMetric(value?: number) {
  if (value == null) return "—";
  return Number.isInteger(value) ? `${value}` : value.toFixed(1);
}

function outcomeKey(market: SoccerMarket) {
  return `${market.outcome}__${market.line ?? "n/a"}`;
}

function parseGame(game: string) {
  const [away, home] = game.split(" @ ");
  return { awayTeam: away ?? game, homeTeam: home ?? "" };
}

function approxRecord(winRate?: number, sampleSize?: number) {
  if (winRate == null || sampleSize == null) return "—";
  const wins = Math.round(winRate * sampleSize);
  return `${wins}-${Math.max(sampleSize - wins, 0)}`;
}

function analyticsForWindow(market: SoccerMarket, window: AnalyticsWindow) {
  if (window === "l3") {
    return {
      homeScored: market.home_l3_goals_scored,
      homeAllowed: market.home_l3_goals_allowed,
      homeWinRate: market.home_l3_win_rate,
      awayScored: market.away_l3_goals_scored,
      awayAllowed: market.away_l3_goals_allowed,
      awayWinRate: market.away_l3_win_rate,
      sampleSize: 3,
    };
  }
  if (window === "l5") {
    return {
      homeScored: market.home_l5_goals_scored,
      homeAllowed: market.home_l5_goals_allowed,
      homeWinRate: market.home_l5_win_rate,
      awayScored: market.away_l5_goals_scored,
      awayAllowed: market.away_l5_goals_allowed,
      awayWinRate: market.away_l5_win_rate,
      sampleSize: 5,
    };
  }
  if (window === "l7") {
    return {
      homeScored: market.home_l7_goals_scored,
      homeAllowed: market.home_l7_goals_allowed,
      homeWinRate: market.home_l7_win_rate,
      awayScored: market.away_l7_goals_scored,
      awayAllowed: market.away_l7_goals_allowed,
      awayWinRate: market.away_l7_win_rate,
      sampleSize: 7,
    };
  }

  return {
    homeScored: market.home_season_gf,
    homeAllowed: market.home_season_ga,
    homeWinRate: undefined,
    awayScored: market.away_season_gf,
    awayAllowed: market.away_season_ga,
    awayWinRate: undefined,
    sampleSize: undefined,
  };
}

export default function SoccerTodayScreen() {
  const { colors } = useTheme();
  const addToBetslip = useSoccerBetslip((s) => s.add);
  const openDrawer = useSoccerBetslipDrawer((s) => s.open);
  const { data, loading, error, refetch } = useEplQuery<SoccerResponse>("/soccer/todays-betting-analysis");

  const [selectedLeague, setSelectedLeague] = useState<LeagueFilter>("MLS");
  const [selectedMarketByGame, setSelectedMarketByGame] = useState<Record<string, NormalizedMarket>>({});
  const [selectedOutcomeByGame, setSelectedOutcomeByGame] = useState<Record<string, string>>({});
  const [selectedWindowByGame, setSelectedWindowByGame] = useState<Record<string, AnalyticsWindow>>({});

  const games = useMemo(() => {
    const grouped = new Map<string, SoccerMarket[]>();
    const orderedGames: string[] = [];

    for (const market of data?.all_markets ?? []) {
      if (!grouped.has(market.game)) {
        grouped.set(market.game, []);
        orderedGames.push(market.game);
      }
      grouped.get(market.game)?.push(market);
    }

    return orderedGames
      .map((game) => ({ game, markets: grouped.get(game) ?? [] }))
      .filter(({ markets }) => normalizeLeague(markets[0]?.league ?? "") === selectedLeague);
  }, [data?.all_markets, selectedLeague]);

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
          <Text style={styles.h1}>MLS Odds + Analytics</Text>
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
        </View>
      }
      data={games}
      keyExtractor={(item) => item.game}
      renderItem={({ item }) => {
        const byMarket = item.markets.reduce<Record<NormalizedMarket, SoccerMarket[]>>(
          (acc, market) => {
            const key = normalizeMarket(market.market);
            if (!acc[key]) acc[key] = [];
            acc[key].push(market);
            return acc;
          },
          { alternate_totals: [], btts: [], draw_no_bet: [], double_chance: [], outright_winner: [], other: [] }
        );

        const availableMarketEntries = ["alternate_totals", "btts", "draw_no_bet", "double_chance", "outright_winner", "other"]
          .map((key) => {
            const typedKey = key as NormalizedMarket;
            const source = byMarket[typedKey] ?? [];
            const values = typedKey === "alternate_totals" ? source.filter(isGoalTotalWithinRange) : source;
            return { key: typedKey, values };
          })
          .filter((entry) => entry.values.length > 0);

        const marketOptions = availableMarketEntries.map((entry) => entry.key);

        if (!marketOptions.length) return null;

        const activeMarket = marketOptions.includes(selectedMarketByGame[item.game])
          ? selectedMarketByGame[item.game]
          : marketOptions[0];
        const candidates = (activeMarket === "alternate_totals"
          ? byMarket[activeMarket].filter(isGoalTotalWithinRange)
          : byMarket[activeMarket]
        )
          .slice()
          .sort((a, b) => (a.line ?? 0) - (b.line ?? 0));
        const selectedOutcomeKey = selectedOutcomeByGame[item.game] ?? outcomeKey(candidates[0]);
        const selectedBet = candidates.find((market) => outcomeKey(market) === selectedOutcomeKey) ?? candidates[0];

        if (!selectedBet) return null;

        const analyticsSource = item.markets.find(hasAnalytics) ?? selectedBet;
        const window = selectedWindowByGame[item.game] ?? "l5";
        const analytics = analyticsForWindow(analyticsSource, window);
        const { awayTeam, homeTeam } = parseGame(item.game);

        const betId = `${selectedBet.game}-${selectedBet.market}-${selectedBet.outcome}-${selectedBet.line ?? "n/a"}`;

        return (
          <View style={[styles.card, { borderColor: colors.border.subtle }]}>
            <ScrollView horizontal showsHorizontalScrollIndicator={false} contentContainerStyle={styles.marketToggleRow}>
              {marketOptions.map((marketKey) => {
                const isSelected = marketKey === activeMarket;
                return (
                  <Pressable
                    key={`${item.game}-${marketKey}`}
                    style={[
                      styles.toggleBtn,
                      {
                        borderColor: isSelected ? "#60A5FA" : colors.border.subtle,
                        backgroundColor: isSelected ? "rgba(96,165,250,0.2)" : "transparent",
                      },
                    ]}
                    onPress={() => {
                      setSelectedMarketByGame((prev) => ({ ...prev, [item.game]: marketKey }));
                      setSelectedOutcomeByGame((prev) => {
                        const next = { ...prev };
                        delete next[item.game];
                        return next;
                      });
                    }}
                  >
                    <Text style={[styles.toggleText, { color: isSelected ? "#DBEAFE" : colors.text.muted }]}>{displayMarket(marketKey)}</Text>
                  </Pressable>
                );
              })}
            </ScrollView>

            <ScrollView horizontal showsHorizontalScrollIndicator={false} contentContainerStyle={styles.marketToggleRow}>
              {candidates.map((market, index) => {
                const key = outcomeKey(market);
                const selected = key === selectedOutcomeKey || (index === 0 && selectedOutcomeByGame[item.game] == null);
                const lineLabel = market.line == null ? "" : ` ${market.line}`;
                return (
                  <Pressable
                    key={`${item.game}-${key}`}
                    style={[
                      styles.toggleBtn,
                      {
                        borderColor: selected ? "#22D3EE" : colors.border.subtle,
                        backgroundColor: selected ? "rgba(34,211,238,0.2)" : "transparent",
                      },
                    ]}
                    onPress={() => setSelectedOutcomeByGame((prev) => ({ ...prev, [item.game]: key }))}
                  >
                    <Text style={[styles.toggleText, { color: selected ? "#CFFAFE" : colors.text.muted }]}>
                      {market.outcome}
                      {lineLabel}
                    </Text>
                  </Pressable>
                );
              })}
            </ScrollView>

            <View style={styles.cardTopRow}>
              <Text style={[styles.game, { color: colors.text.primary }]}>{awayTeam} @ {homeTeam}</Text>
              <View style={styles.pricePill}>
                <Text style={styles.priceText}>{formatPrice(selectedBet.best_price)}</Text>
              </View>
            </View>

            <Text style={[styles.meta, { color: colors.text.muted }]}>Start: {selectedBet.start_time_et ?? "TBD"} • {selectedBet.best_bookmaker}</Text>

            <View style={[styles.analyticsBox, { borderColor: colors.border.subtle }]}> 
              <ScrollView horizontal showsHorizontalScrollIndicator={false} contentContainerStyle={styles.marketToggleRow}>
                {WINDOW_TABS.map((windowKey) => {
                  const isSelected = windowKey === window;
                  return (
                    <Pressable
                      key={`${item.game}-${windowKey}`}
                      style={[
                        styles.toggleBtn,
                        {
                          borderColor: isSelected ? "#4ADE80" : colors.border.subtle,
                          backgroundColor: isSelected ? "rgba(74,222,128,0.2)" : "transparent",
                        },
                      ]}
                      onPress={() => setSelectedWindowByGame((prev) => ({ ...prev, [item.game]: windowKey }))}
                    >
                      <Text style={[styles.toggleText, { color: isSelected ? "#DCFCE7" : colors.text.muted }]}>{windowKey}</Text>
                    </Pressable>
                  );
                })}
              </ScrollView>

              <View style={styles.metricsRow}>
                <Text style={[styles.metric, { color: colors.text.primary }]}>Home GF {formatMetric(analytics.homeScored)}</Text>
                <Text style={[styles.metric, { color: colors.text.primary }]}>Home GA {formatMetric(analytics.homeAllowed)}</Text>
                <Text style={[styles.metric, { color: colors.text.primary }]}>Away GF {formatMetric(analytics.awayScored)}</Text>
                <Text style={[styles.metric, { color: colors.text.primary }]}>Away GA {formatMetric(analytics.awayAllowed)}</Text>
                <Text style={[styles.metric, { color: colors.text.primary }]}>Home W/L {approxRecord(analytics.homeWinRate, analytics.sampleSize)}</Text>
                <Text style={[styles.metric, { color: colors.text.primary }]}>Away W/L {approxRecord(analytics.awayWinRate, analytics.sampleSize)}</Text>
                <Text style={[styles.metric, { color: colors.text.muted }]}>Home Win {formatPercent(analytics.homeWinRate)}</Text>
                <Text style={[styles.metric, { color: colors.text.muted }]}>Away Win {formatPercent(analytics.awayWinRate)}</Text>
              </View>

              <View style={styles.metricsRow}>
                <Text style={[styles.metric, { color: colors.text.muted }]}>Implied {formatPercent(selectedBet.implied_prob)}</Text>
                <Text style={[styles.metric, { color: colors.text.muted }]}>Model {formatPercent(selectedBet.model_confidence)}</Text>
                <Text style={[styles.metric, { color: "#4ADE80" }]}>Edge {formatPercent(selectedBet.edge)}</Text>
              </View>
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
          <Text style={[styles.market, { color: colors.text.primary }]}>No {selectedLeague} odds found yet.</Text>
          <Text style={[styles.meta, { color: colors.text.muted }]}>Try refresh once the ingestion job finishes.</Text>
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
    gap: 8,
  },
  cardTopRow: { flexDirection: "row", alignItems: "center", justifyContent: "space-between" },
  game: { fontWeight: "700", fontSize: 14, flex: 1, paddingRight: 8 },
  market: { fontWeight: "700", fontSize: 13 },
  meta: { fontSize: 12 },
  marketToggleRow: { flexDirection: "row", gap: 8, paddingRight: 8 },
  toggleBtn: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 999,
    paddingHorizontal: 10,
    paddingVertical: 6,
  },
  toggleText: { fontSize: 11, fontWeight: "700" },
  pricePill: { backgroundColor: "rgba(74,222,128,0.2)", borderRadius: 999, paddingHorizontal: 10, paddingVertical: 4 },
  priceText: { color: "#86EFAC", fontWeight: "800", fontSize: 12 },
  analyticsBox: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 10,
    padding: 10,
    gap: 8,
    backgroundColor: "rgba(148, 163, 184, 0.08)",
  },
  metricsRow: { flexDirection: "row", flexWrap: "wrap", gap: 10 },
  metric: { fontSize: 11, fontWeight: "600" },
  actionsRow: { flexDirection: "row", gap: 8, marginTop: 4 },
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

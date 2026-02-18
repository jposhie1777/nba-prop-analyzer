import { useMemo } from "react";
import { ActivityIndicator, FlatList, Pressable, StyleSheet, Text, View } from "react-native";

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

function formatPct(value?: number) {
  if (value == null) return "-";
  return `${(value * 100).toFixed(1)}%`;
}

export default function SoccerTodayScreen() {
  const { colors } = useTheme();
  const addToBetslip = useSoccerBetslip((s) => s.add);
  const openDrawer = useSoccerBetslipDrawer((s) => s.open);
  const { data, loading, error, refetch } = useEplQuery<SoccerResponse>("/soccer/todays-betting-analysis");

  const suggestions = useMemo(() => data?.suggestions ?? [], [data?.suggestions]);

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
      data={suggestions}
      keyExtractor={(item, idx) => `${item.game}-${item.market}-${item.outcome}-${idx}`}
      renderItem={({ item }) => {
        const betId = `${item.game}-${item.market}-${item.outcome}-${item.line ?? "n/a"}`;
        return (
          <View style={[styles.card, { borderColor: colors.border.subtle }]}> 
            <View style={styles.cardTopRow}>
              <Text style={[styles.game, { color: colors.text.primary }]}>{item.game}</Text>
              <View style={styles.pricePill}>
                <Text style={styles.priceText}>{item.best_price > 0 ? `+${item.best_price}` : `${item.best_price}`}</Text>
              </View>
            </View>

            <Text style={[styles.market, { color: colors.text.primary }]}>
              {item.market}: {item.outcome}
              {item.line != null ? ` (${item.line})` : ""}
            </Text>

            <Text style={[styles.meta, { color: colors.text.muted }]}>
              {item.league} • {item.best_bookmaker}
            </Text>

            <View style={styles.metricsRow}>
              <Text style={[styles.metric, { color: colors.text.muted }]}>Implied {formatPct(item.implied_prob)}</Text>
              <Text style={[styles.metric, { color: colors.text.muted }]}>Model {formatPct(item.model_confidence)}</Text>
              <Text style={[styles.metric, { color: "#4ADE80" }]}>Edge {formatPct(item.edge)}</Text>
            </View>

            {!!item.rationale && (
              <Text style={[styles.rationale, { color: colors.text.muted }]}>{item.rationale}</Text>
            )}

            <View style={styles.actionsRow}>
              <Pressable
                style={[styles.primaryBtn, { backgroundColor: "#1E40AF" }]}
                onPress={() => {
                  addToBetslip({
                    id: betId,
                    league: item.league,
                    game: item.game,
                    start_time_et: item.start_time_et,
                    market: item.market,
                    outcome: item.outcome,
                    line: item.line,
                    price: item.best_price,
                    bookmaker: item.best_bookmaker,
                    rationale: item.rationale,
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

import { useMemo, useState } from "react";
import { ActivityIndicator, FlatList, Linking, Pressable, ScrollView, StyleSheet, Text, View } from "react-native";
import * as Clipboard from "expo-clipboard";

import {
  EplBettingAnalyticsRow,
  useEplBettingAnalytics,
} from "@/hooks/epl/useEplBettingAnalytics";
import { useTheme } from "@/store/useTheme";
import { useSoccerBetslip } from "@/store/useSoccerBetslip";
import { useSoccerBetslipDrawer } from "@/store/useSoccerBetslipDrawer";

const GAMBLY_URL = "https://www.gambly.com/gambly-bot";

function formatPct(value?: number | null) {
  if (value == null) return "-";
  return `${(value * 100).toFixed(1)}%`;
}

function formatPrice(value?: number | null) {
  if (value == null) return "-";
  return value > 0 ? `+${value}` : `${value}`;
}

function formatLine(value?: number | null) {
  return value == null ? "" : ` (${value})`;
}

function formatMetric(value?: number | null, digits: number = 2) {
  if (value == null) return "-";
  return Number.isInteger(value) ? `${value}` : value.toFixed(digits);
}

function toSlipText(row: EplBettingAnalyticsRow) {
  return `${row.game} — ${row.market}: ${row.outcome}${formatLine(row.line)} @ ${formatPrice(row.price)} (${row.bookmaker})`;
}

export default function EplBettingAnalyticsScreen() {
  const { colors } = useTheme();
  const addToBetslip = useSoccerBetslip((s) => s.add);
  const openDrawer = useSoccerBetslipDrawer((s) => s.open);
  const [selectedMarket, setSelectedMarket] = useState<string>("all");
  const [selectedBook, setSelectedBook] = useState<string>("all");
  const [onlyBestPrice, setOnlyBestPrice] = useState(true);

  const { data, loading, error, refetch } = useEplBettingAnalytics({
    market: selectedMarket === "all" ? undefined : selectedMarket,
    bookmaker: selectedBook === "all" ? undefined : selectedBook,
    only_best_price: onlyBestPrice,
    limit: 300,
  });

  const markets = useMemo(() => ["all", ...(data?.available_markets ?? [])], [data?.available_markets]);
  const books = useMemo(() => ["all", ...(data?.available_bookmakers ?? [])], [data?.available_bookmakers]);
  const rows = data?.rows ?? [];

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
        <Text style={{ color: "#fff", textAlign: "center" }}>Failed to load EPL betting analytics: {error}</Text>
        <Pressable onPress={refetch} style={[styles.retryBtn, { borderColor: colors.border.subtle }]}> 
          <Text style={{ color: colors.text.primary, fontWeight: "700" }}>Retry</Text>
        </Pressable>
      </View>
    );
  }

  return (
    <FlatList
      style={{ flex: 1, backgroundColor: "#050A18" }}
      contentContainerStyle={{ padding: 12, paddingBottom: 40, gap: 10 }}
      data={rows}
      keyExtractor={(item, idx) => `${item.game}-${item.market}-${item.outcome}-${item.line ?? "na"}-${item.bookmaker}-${idx}`}
      ListHeaderComponent={
        <View style={[styles.hero, { borderColor: colors.border.subtle }]}> 
          <Text style={styles.eyebrow}>EPL BETTING ANALYTICS</Text>
          <Text style={styles.h1}>Live Bet Board</Text>
          <Text style={[styles.sub, { color: colors.text.muted }]}> 
            Date: {data?.date_et ?? "-"} • Rows: {data?.row_count ?? 0}
          </Text>

          <Text style={[styles.filterLabel, { color: colors.text.muted }]}>Market</Text>
          <ScrollView horizontal showsHorizontalScrollIndicator={false} contentContainerStyle={styles.pillRow}>
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
                      backgroundColor: active ? "rgba(34,211,238,0.22)" : "transparent",
                    },
                  ]}
                >
                  <Text style={[styles.pillText, { color: active ? "#CFFAFE" : colors.text.muted }]}>{market}</Text>
                </Pressable>
              );
            })}
          </ScrollView>

          <Text style={[styles.filterLabel, { color: colors.text.muted }]}>Bookmaker</Text>
          <ScrollView horizontal showsHorizontalScrollIndicator={false} contentContainerStyle={styles.pillRow}>
            {books.map((book) => {
              const active = selectedBook === book;
              return (
                <Pressable
                  key={book}
                  onPress={() => setSelectedBook(book)}
                  style={[
                    styles.pill,
                    {
                      borderColor: active ? "#A78BFA" : colors.border.subtle,
                      backgroundColor: active ? "rgba(167,139,250,0.22)" : "transparent",
                    },
                  ]}
                >
                  <Text style={[styles.pillText, { color: active ? "#E9D5FF" : colors.text.muted }]}>{book}</Text>
                </Pressable>
              );
            })}
          </ScrollView>

          <Pressable
            style={[styles.bestPriceToggle, { borderColor: colors.border.subtle }]}
            onPress={() => setOnlyBestPrice((v) => !v)}
          >
            <Text style={{ color: colors.text.primary, fontWeight: "700", fontSize: 12 }}>
              {onlyBestPrice ? "Showing Best Price Only" : "Showing All Prices"}
            </Text>
          </Pressable>
        </View>
      }
      renderItem={({ item }) => {
        const betId = `${item.game}-${item.market}-${item.outcome}-${item.line ?? "na"}`;
        return (
          <View style={[styles.card, { borderColor: colors.border.subtle }]}> 
            <View style={styles.rowBetween}>
              <Text style={[styles.game, { color: colors.text.primary }]}>{item.game}</Text>
              <View style={[styles.pricePill, { backgroundColor: "rgba(74,222,128,0.20)" }]}>
                <Text style={styles.pricePillText}>{formatPrice(item.price)}</Text>
              </View>
            </View>

            <Text style={[styles.market, { color: colors.text.primary }]}>
              {item.market}: {item.outcome}
              {formatLine(item.line)}
            </Text>
            <Text style={[styles.meta, { color: colors.text.muted }]}>
              {item.bookmaker} • Rank #{item.price_rank ?? "-"} • Best Price {item.is_best_price ? "Yes" : "No"}
            </Text>

            <View style={[styles.analyticsBox, { borderColor: colors.border.subtle }]}> 
              <View style={styles.metricsRow}>
                <Text style={[styles.metric, { color: colors.text.muted }]}>Implied {formatPct(item.implied_probability)}</Text>
                <Text style={[styles.metric, { color: colors.text.muted }]}>No-vig {formatPct(item.no_vig_probability)}</Text>
                <Text style={[styles.metric, { color: "#4ADE80" }]}>Vs Mkt {formatPct(item.probability_vs_market)}</Text>
              </View>
              <View style={styles.metricsRow}>
                <Text style={[styles.metric, { color: colors.text.muted }]}>Hold {formatPct(item.market_hold)}</Text>
                <Text style={[styles.metric, { color: colors.text.muted }]}>Exp G {formatMetric(item.model_expected_total_goals)}</Text>
                <Text style={[styles.metric, { color: colors.text.muted }]}>Edge {formatMetric(item.model_total_line_edge)}</Text>
              </View>
              <Text style={[styles.meta, { color: colors.text.muted }]}>Edge Tier: {item.model_edge_tier ?? "-"}</Text>
            </View>

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
                    price: item.price,
                    bookmaker: item.bookmaker,
                    rationale: `Edge vs market: ${formatPct(item.probability_vs_market)} | Tier: ${item.model_edge_tier ?? "-"}`,
                  });
                }}
              >
                <Text style={styles.primaryBtnText}>Save to Betslip</Text>
              </Pressable>

              <Pressable
                style={[styles.secondaryBtn, { borderColor: colors.border.subtle }]}
                onPress={async () => {
                  await Clipboard.setStringAsync(toSlipText(item));
                  openDrawer();
                }}
              >
                <Text style={[styles.secondaryBtnText, { color: colors.text.primary }]}>Copy for Gambly</Text>
              </Pressable>
            </View>

            <Pressable
              style={[styles.secondaryBtn, { borderColor: colors.border.subtle, marginTop: 8 }]}
              onPress={() => Linking.openURL(GAMBLY_URL)}
            >
              <Text style={[styles.secondaryBtnText, { color: colors.text.primary }]}>Open Gambly</Text>
            </Pressable>
          </View>
        );
      }}
      ListEmptyComponent={
        <View style={[styles.card, { borderColor: colors.border.subtle }]}> 
          <Text style={[styles.market, { color: colors.text.primary }]}>No rows found.</Text>
          <Text style={[styles.meta, { color: colors.text.muted }]}>Try changing market/bookmaker filters or disabling best price only.</Text>
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
  filterLabel: { fontSize: 11, fontWeight: "700", marginTop: 12 },
  pillRow: { gap: 8, paddingTop: 8, paddingRight: 12 },
  pill: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 999,
    paddingHorizontal: 12,
    paddingVertical: 7,
  },
  pillText: { fontSize: 11, fontWeight: "700" },
  bestPriceToggle: {
    marginTop: 12,
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 10,
    alignItems: "center",
    paddingVertical: 8,
  },
  card: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 14,
    backgroundColor: "#0B1529",
    padding: 12,
    gap: 6,
  },
  rowBetween: { flexDirection: "row", alignItems: "center", justifyContent: "space-between" },
  game: { fontWeight: "700", fontSize: 14, flex: 1, paddingRight: 8 },
  market: { fontWeight: "700", fontSize: 13 },
  meta: { fontSize: 12 },
  pricePill: { borderRadius: 999, paddingHorizontal: 10, paddingVertical: 4 },
  pricePillText: { color: "#86EFAC", fontWeight: "800", fontSize: 12 },
  analyticsBox: {
    marginTop: 6,
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 10,
    padding: 10,
    gap: 6,
    backgroundColor: "rgba(148, 163, 184, 0.08)",
  },
  metricsRow: { flexDirection: "row", flexWrap: "wrap", gap: 10 },
  metric: { fontSize: 12, fontWeight: "600" },
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

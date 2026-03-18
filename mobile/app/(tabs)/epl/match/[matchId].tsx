import { ActivityIndicator, Pressable, ScrollView, StyleSheet, Text, View } from "react-native";
import { useLocalSearchParams, useRouter } from "expo-router";

import { useEplOddspediaMatchDetail } from "@/hooks/epl/useEplOddspedia";
import { useTheme } from "@/store/useTheme";

function formatAmerican(price?: number | null) {
  if (price == null) return "–";
  return price > 0 ? `+${price}` : `${price}`;
}

export default function EplMatchDetailScreen() {
  const router = useRouter();
  const { colors } = useTheme();
  const params = useLocalSearchParams<{ matchId?: string }>();
  const matchId = Number(params.matchId);
  const { data, loading, error, refetch } = useEplOddspediaMatchDetail(Number.isFinite(matchId) ? matchId : null);

  return (
    <ScrollView style={styles.screen} contentContainerStyle={styles.content}>
      <Pressable onPress={() => router.back()} style={[styles.backButton, { borderColor: colors.border.subtle }]}> 
        <Text style={styles.backButtonText}>← Back</Text>
      </Pressable>

      {loading ? <ActivityIndicator color="#A78BFA" /> : null}
      {error ? (
        <Pressable onPress={refetch} style={[styles.panel, { borderColor: colors.border.subtle }]}>
          <Text style={styles.errorTitle}>Failed to load analytics.</Text>
          <Text style={styles.errorText}>{error}</Text>
        </Pressable>
      ) : null}

      {data ? (
        <>
          <View style={[styles.panel, { borderColor: colors.border.subtle }]}>
            <Text style={styles.title}>{data.match.home_team} vs {data.match.away_team}</Text>
            <Text style={styles.meta}>Additional Odds</Text>
            {(data.match.additional_odds || []).map((group) => (
              <View key={group.market} style={styles.subSection}>
                <Text style={styles.subSectionTitle}>{group.market.toUpperCase()}</Text>
                {group.outcomes.map((o, idx) => (
                  <Text key={`${group.market}-${idx}`} style={styles.lineItem}>
                    {o.outcome_name}{o.line_value ? ` ${o.line_value}` : ""}: {formatAmerican(o.odds_american)}
                  </Text>
                ))}
              </View>
            ))}
          </View>

          <View style={[styles.panel, { borderColor: colors.border.subtle }]}>
            <Text style={styles.meta}>H2H History</Text>
            {data.head_to_head.length ? data.head_to_head.slice(0, 8).map((item, idx) => (
              <Text key={`h2h-${idx}`} style={styles.lineItem}>
                {item.h2h_ht} {item.h2h_hscore} - {item.h2h_ascore} {item.h2h_at}
              </Text>
            )) : <Text style={styles.empty}>No H2H data available.</Text>}
          </View>

          <View style={[styles.panel, { borderColor: colors.border.subtle }]}>
            <Text style={styles.meta}>Recent Matches / Form</Text>
            {data.last_matches.length ? data.last_matches.slice(0, 12).map((item, idx) => (
              <Text key={`lm-${idx}`} style={styles.lineItem}>
                [{item.side}] {item.lm_ht} {item.lm_hscore} - {item.lm_ascore} {item.lm_at} ({item.lm_outcome ?? "N/A"})
              </Text>
            )) : <Text style={styles.empty}>No recent form data available.</Text>}
          </View>

          <View style={[styles.panel, { borderColor: colors.border.subtle }]}>
            <Text style={styles.meta}>Betting Trends</Text>
            {data.betting_trends.length ? data.betting_trends.map((trend, idx) => (
              <Text key={`trend-${idx}`} style={styles.lineItem}>
                #{trend.rank ?? idx + 1} {trend.statement}
              </Text>
            )) : <Text style={styles.empty}>No betting trends available.</Text>}
          </View>

          <View style={[styles.panel, { borderColor: colors.border.subtle }]}>
            <Text style={styles.meta}>Betting Stats</Text>
            {data.betting_stats.length ? data.betting_stats.slice(0, 20).map((row, idx) => (
              <Text key={`bs-${idx}`} style={styles.lineItem}>
                {row.category} • {row.sub_tab} • {row.label}: {row.value}
              </Text>
            )) : <Text style={styles.empty}>No betting stats available.</Text>}
          </View>
        </>
      ) : null}
    </ScrollView>
  );
}

const styles = StyleSheet.create({
  screen: { flex: 1, backgroundColor: "#050A18" },
  content: { padding: 16, gap: 10, paddingBottom: 40 },
  backButton: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 12,
    backgroundColor: "#0B1529",
    paddingHorizontal: 12,
    paddingVertical: 8,
    alignSelf: "flex-start",
  },
  backButtonText: { color: "#E9F2FF", fontWeight: "700" },
  panel: { borderWidth: StyleSheet.hairlineWidth, borderRadius: 14, backgroundColor: "#0B1529", padding: 12, gap: 8 },
  title: { color: "#E9F2FF", fontSize: 18, fontWeight: "800" },
  meta: { color: "#93C5FD", fontSize: 14, fontWeight: "700" },
  subSection: { gap: 4 },
  subSectionTitle: { color: "#C4B5FD", fontWeight: "700", fontSize: 12 },
  lineItem: { color: "#E5E7EB", fontSize: 12 },
  empty: { color: "#94A3B8", fontSize: 12 },
  errorTitle: { color: "#FCA5A5", fontWeight: "700" },
  errorText: { color: "#FECACA", fontSize: 12 },
});

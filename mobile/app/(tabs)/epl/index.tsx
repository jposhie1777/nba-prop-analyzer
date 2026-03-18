import { ActivityIndicator, Image, Pressable, ScrollView, StyleSheet, Text, View } from "react-native";
import { useRouter } from "expo-router";

import { useEplOddspediaMatches } from "@/hooks/epl/useEplOddspedia";
import { useTheme } from "@/store/useTheme";

function formatDate(value?: string | null) {
  if (!value) return "TBD";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "TBD";
  return date.toLocaleString(undefined, {
    weekday: "short",
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

function formatAmerican(price?: number | null) {
  if (price == null) return "–";
  return price > 0 ? `+${price}` : `${price}`;
}

function OddsRow({ label, odds }: { label: string; odds: Array<{ outcome_name: string; odds_american?: number | null; line_value?: string | null }> }) {
  return (
    <View style={styles.marketRow}>
      <Text style={styles.marketLabel}>{label}</Text>
      <Text style={styles.marketValue} numberOfLines={1}>
        {odds.length
          ? odds
              .map((o) => `${o.outcome_name}${o.line_value ? ` ${o.line_value}` : ""} ${formatAmerican(o.odds_american)}`)
              .join("  •  ")
          : "Not available"}
      </Text>
    </View>
  );
}

export default function EplHome() {
  const router = useRouter();
  const { colors } = useTheme();
  const { data, loading, error, refetch } = useEplOddspediaMatches(120);

  return (
    <ScrollView style={styles.screen} contentContainerStyle={styles.content}>
      <View style={[styles.hero, { borderColor: colors.border.subtle }]}> 
        <Text style={styles.eyebrow}>EPL</Text>
        <Text style={styles.h1}>Matches</Text>
        <Text style={styles.sub}>Ordered by date and start time (most recent first).</Text>
      </View>

      {loading ? <ActivityIndicator color="#A78BFA" /> : null}
      {error ? (
        <Pressable onPress={refetch} style={[styles.errorBox, { borderColor: colors.border.subtle }]}>
          <Text style={styles.errorTitle}>Failed to load matches.</Text>
          <Text style={styles.errorText}>{error}</Text>
          <Text style={styles.errorRetry}>Tap to retry</Text>
        </Pressable>
      ) : null}

      {(data ?? []).map((match) => (
        <Pressable
          key={match.match_id}
          onPress={() => router.push(`/(tabs)/epl/match/${match.match_id}`)}
          style={[styles.card, { borderColor: colors.border.subtle }]}
        >
          <View style={styles.teamsRow}>
            <View style={styles.teamWrap}>
              <Image source={{ uri: match.home_logo || undefined }} style={styles.logo} />
              <Text style={styles.teamName}>{match.home_team}</Text>
            </View>
            <Text style={styles.vs}>vs</Text>
            <View style={[styles.teamWrap, { alignItems: "flex-end" }]}>
              <Image source={{ uri: match.away_logo || undefined }} style={styles.logo} />
              <Text style={[styles.teamName, { textAlign: "right" }]}>{match.away_team}</Text>
            </View>
          </View>

          <Text style={styles.timeText}>{formatDate(match.date_utc)}</Text>

          <OddsRow label="ML" odds={match.main_odds.h2h} />
          <OddsRow label="Spread" odds={match.main_odds.spreads} />
          <OddsRow label="Totals" odds={match.main_odds.totals} />
        </Pressable>
      ))}
    </ScrollView>
  );
}

const styles = StyleSheet.create({
  screen: { flex: 1, backgroundColor: "#050A18" },
  content: { padding: 16, gap: 10, paddingBottom: 40 },
  hero: { borderWidth: StyleSheet.hairlineWidth, borderRadius: 16, backgroundColor: "#071731", padding: 16, marginBottom: 8 },
  eyebrow: { color: "#90B3E9", fontSize: 11, fontWeight: "700" },
  h1: { color: "#E9F2FF", fontSize: 24, fontWeight: "800", marginTop: 8 },
  sub: { color: "#A7C0E8", marginTop: 6, lineHeight: 18, fontSize: 13 },
  card: { borderWidth: StyleSheet.hairlineWidth, borderRadius: 14, backgroundColor: "#0B1529", padding: 12, gap: 8 },
  teamsRow: { flexDirection: "row", alignItems: "center", justifyContent: "space-between", gap: 8 },
  teamWrap: { flex: 1, flexDirection: "row", alignItems: "center", gap: 8 },
  logo: { width: 26, height: 26, borderRadius: 13, backgroundColor: "#111827" },
  teamName: { color: "#E5E7EB", fontSize: 13, fontWeight: "700", flexShrink: 1 },
  vs: { color: "#A7C0E8", fontWeight: "700" },
  timeText: { color: "#CBD5E1", fontSize: 12 },
  marketRow: { flexDirection: "row", gap: 8 },
  marketLabel: { color: "#93C5FD", width: 52, fontWeight: "700", fontSize: 12 },
  marketValue: { color: "#E5E7EB", flex: 1, fontSize: 12 },
  errorBox: { borderWidth: StyleSheet.hairlineWidth, borderRadius: 12, backgroundColor: "#1F2937", padding: 12 },
  errorTitle: { color: "#FCA5A5", fontWeight: "700" },
  errorText: { color: "#FECACA", marginTop: 4, fontSize: 12 },
  errorRetry: { color: "#E5E7EB", marginTop: 8, fontSize: 12 },
});

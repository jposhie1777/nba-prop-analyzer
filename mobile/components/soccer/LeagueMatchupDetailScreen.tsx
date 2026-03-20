import { ActivityIndicator, Pressable, ScrollView, StyleSheet, Text, View } from "react-native";
import { useLocalSearchParams, useRouter } from "expo-router";

import { SoccerLeague, useSoccerMatchupDetail } from "@/hooks/soccer/useSoccerMatchups";
import { useTheme } from "@/store/useTheme";

type Props = {
  league: SoccerLeague;
  leagueTitle: string;
};

function formatDateTime(value?: string | null) {
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

function stringifyValue(value: unknown): string {
  if (value == null) return "–";
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

export function LeagueMatchupDetailScreen({ league, leagueTitle }: Props) {
  const router = useRouter();
  const { colors } = useTheme();
  const params = useLocalSearchParams<{ matchId?: string }>();
  const matchId = Number(params.matchId);
  const { data, loading, error, refetch } = useSoccerMatchupDetail(league, Number.isFinite(matchId) ? matchId : null);

  const matchInfo = (data?.match_info ?? {}) as Record<string, unknown>;
  const homeTeam = (matchInfo.home_team as string | undefined) || "Home";
  const awayTeam = (matchInfo.away_team as string | undefined) || "Away";
  const dateUtc = matchInfo.date_utc as string | undefined;

  const matchInfoRows = Object.entries(matchInfo).filter(([key]) => !["match_id", "home_team", "away_team"].includes(key));

  return (
    <ScrollView style={styles.screen} contentContainerStyle={styles.content}>
      <View style={styles.actionRow}>
        <Pressable onPress={() => router.back()} style={[styles.actionButton, { borderColor: colors.border.subtle }]}>
          <Text style={styles.actionText}>← Back</Text>
        </Pressable>
        <Pressable onPress={() => router.push("/(tabs)/home")} style={[styles.actionButton, { borderColor: colors.border.subtle }]}>
          <Text style={styles.actionText}>⌂ Home</Text>
        </Pressable>
      </View>

      <View style={[styles.panel, { borderColor: colors.border.subtle }]}>
        <Text style={styles.eyebrow}>{leagueTitle}</Text>
        <Text style={styles.title}>{homeTeam} vs {awayTeam}</Text>
        <Text style={styles.subtitle}>{formatDateTime(dateUtc ?? null)}</Text>
      </View>

      {loading ? <ActivityIndicator color="#A78BFA" /> : null}

      {error ? (
        <Pressable onPress={refetch} style={[styles.panel, { borderColor: colors.border.subtle }]}>
          <Text style={styles.errorTitle}>Failed to load matchup detail.</Text>
          <Text style={styles.errorText}>{error}</Text>
          <Text style={styles.errorRetry}>Tap to retry</Text>
        </Pressable>
      ) : null}

      {data ? (
        <>
          <View style={[styles.panel, { borderColor: colors.border.subtle }]}>
            <Text style={styles.sectionTitle}>Match Info</Text>
            {matchInfoRows.length ? (
              matchInfoRows.map(([key, value]) => (
                <View key={key} style={styles.row}>
                  <Text style={styles.keyText}>{key}</Text>
                  <Text style={styles.valueText}>{stringifyValue(value)}</Text>
                </View>
              ))
            ) : (
              <Text style={styles.emptyText}>No match info available.</Text>
            )}
          </View>

          <View style={[styles.panel, { borderColor: colors.border.subtle }]}>
            <Text style={styles.sectionTitle}>Match Keys - Betting Insights</Text>
            {data.match_keys.length ? (
              data.match_keys.map((row, idx) => (
                <Text key={`mk-${idx}`} style={styles.valueText}>
                  #{row.rank ?? idx + 1} {row.statement ?? "No statement"}
                </Text>
              ))
            ) : (
              <Text style={styles.emptyText}>No match keys available.</Text>
            )}
          </View>

          <View style={[styles.panel, { borderColor: colors.border.subtle }]}>
            <Text style={styles.sectionTitle}>Betting Stats</Text>
            {data.betting_stats.length ? (
              data.betting_stats.map((row, idx) => (
                <Text key={`bs-${idx}`} style={styles.valueText}>
                  {row.category ?? "category"} • {row.sub_tab ?? "sub_tab"} • {row.label ?? "label"}: {row.value ?? "–"}
                </Text>
              ))
            ) : (
              <Text style={styles.emptyText}>No betting stats available.</Text>
            )}
          </View>

          <View style={[styles.panel, { borderColor: colors.border.subtle }]}>
            <Text style={styles.sectionTitle}>Last Matches</Text>
            {data.last_matches.length ? (
              data.last_matches.map((row, idx) => (
                <Text key={`lm-${idx}`} style={styles.valueText}>
                  [{row.side ?? "side"}] {row.lm_ht ?? "Home"} {row.lm_hscore ?? "–"} - {row.lm_ascore ?? "–"} {row.lm_at ?? "Away"} ({row.lm_outcome ?? "N/A"})
                </Text>
              ))
            ) : (
              <Text style={styles.emptyText}>No last matches available.</Text>
            )}
          </View>
        </>
      ) : null}
    </ScrollView>
  );
}

const styles = StyleSheet.create({
  screen: { flex: 1, backgroundColor: "#050A18" },
  content: { padding: 16, gap: 10, paddingBottom: 40 },
  actionRow: { flexDirection: "row", gap: 8 },
  actionButton: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 12,
    backgroundColor: "#0B1529",
    paddingHorizontal: 12,
    paddingVertical: 8,
  },
  actionText: { color: "#E9F2FF", fontWeight: "700", fontSize: 12 },
  panel: { borderWidth: StyleSheet.hairlineWidth, borderRadius: 14, backgroundColor: "#0B1529", padding: 12, gap: 8 },
  eyebrow: { color: "#90B3E9", fontSize: 11, fontWeight: "700" },
  title: { color: "#E9F2FF", fontSize: 18, fontWeight: "800" },
  subtitle: { color: "#A7C0E8", fontSize: 12 },
  sectionTitle: { color: "#93C5FD", fontSize: 14, fontWeight: "700" },
  row: { gap: 2 },
  keyText: { color: "#C4B5FD", fontSize: 11, fontWeight: "700" },
  valueText: { color: "#E5E7EB", fontSize: 12, lineHeight: 18 },
  emptyText: { color: "#94A3B8", fontSize: 12 },
  errorTitle: { color: "#FCA5A5", fontWeight: "700" },
  errorText: { color: "#FECACA", fontSize: 12 },
  errorRetry: { color: "#E5E7EB", marginTop: 4, fontSize: 12 },
});

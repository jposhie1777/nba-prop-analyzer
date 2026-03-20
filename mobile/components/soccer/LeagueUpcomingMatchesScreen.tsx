import { ActivityIndicator, Pressable, ScrollView, StyleSheet, Text, View } from "react-native";
import { useRouter } from "expo-router";

import { SoccerLeague, useSoccerUpcomingMatches } from "@/hooks/soccer/useSoccerMatchups";
import { useTheme } from "@/store/useTheme";

type Props = {
  league: SoccerLeague;
  title: string;
};

function formatDateOnly(value?: string | null) {
  if (!value) return "TBD";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "TBD";
  return date.toLocaleDateString(undefined, {
    weekday: "short",
    month: "short",
    day: "numeric",
  });
}

function formatTimeOnly(value?: string | null) {
  if (!value) return "TBD";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "TBD";
  return date.toLocaleTimeString(undefined, {
    hour: "numeric",
    minute: "2-digit",
  });
}

export function LeagueUpcomingMatchesScreen({ league, title }: Props) {
  const { colors } = useTheme();
  const router = useRouter();
  const { data, loading, error, refetch } = useSoccerUpcomingMatches(league);

  return (
    <ScrollView style={styles.screen} contentContainerStyle={styles.content}>
      <View style={[styles.hero, { borderColor: colors.border.subtle }]}>
        <Text style={styles.eyebrow}>{title}</Text>
        <Text style={styles.h1}>Upcoming Matches</Text>
        <Text style={styles.sub}>Tap any matchup card for detailed breakdown.</Text>
      </View>

      {loading ? <ActivityIndicator color="#A78BFA" /> : null}

      {error ? (
        <Pressable onPress={refetch} style={[styles.errorBox, { borderColor: colors.border.subtle }]}>
          <Text style={styles.errorTitle}>Failed to load upcoming matches.</Text>
          <Text style={styles.errorText}>{error}</Text>
          <Text style={styles.errorRetry}>Tap to retry</Text>
        </Pressable>
      ) : null}

      {(data ?? []).map((match) => (
        <Pressable
          key={match.match_id}
          onPress={() => router.push(`/(tabs)/${league}/match/${match.match_id}`)}
          style={[styles.card, { borderColor: colors.border.subtle }]}
        >
          <Text style={styles.matchupText}>
            {match.matchup || `${match.home_team ?? "Home"} vs ${match.away_team ?? "Away"}`}
          </Text>
          <View style={styles.metaRow}>
            <Text style={styles.metaLabel}>Date:</Text>
            <Text style={styles.metaValue}>{formatDateOnly(match.start_time_utc)}</Text>
          </View>
          <View style={styles.metaRow}>
            <Text style={styles.metaLabel}>Time:</Text>
            <Text style={styles.metaValue}>{formatTimeOnly(match.start_time_utc)}</Text>
          </View>
        </Pressable>
      ))}

      {!loading && !error && (data ?? []).length === 0 ? (
        <View style={[styles.emptyCard, { borderColor: colors.border.subtle }]}>
          <Text style={styles.emptyTitle}>No upcoming matches available.</Text>
          <Text style={styles.emptySub}>Once new match rows arrive, they will show here automatically.</Text>
        </View>
      ) : null}
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
  matchupText: { color: "#E9F2FF", fontWeight: "800", fontSize: 15 },
  metaRow: { flexDirection: "row", gap: 8 },
  metaLabel: { color: "#93C5FD", width: 42, fontWeight: "700", fontSize: 12 },
  metaValue: { color: "#E5E7EB", fontSize: 12, flex: 1 },
  errorBox: { borderWidth: StyleSheet.hairlineWidth, borderRadius: 12, backgroundColor: "#1F2937", padding: 12 },
  errorTitle: { color: "#FCA5A5", fontWeight: "700" },
  errorText: { color: "#FECACA", marginTop: 4, fontSize: 12 },
  errorRetry: { color: "#E5E7EB", marginTop: 8, fontSize: 12 },
  emptyCard: { borderWidth: StyleSheet.hairlineWidth, borderRadius: 12, backgroundColor: "#0B1529", padding: 14 },
  emptyTitle: { color: "#E5E7EB", fontWeight: "700" },
  emptySub: { color: "#A7C0E8", marginTop: 6, fontSize: 12 },
});

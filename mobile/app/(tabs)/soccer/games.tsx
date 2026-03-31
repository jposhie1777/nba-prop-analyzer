import { useCallback, useMemo } from "react";
import {
  ActivityIndicator,
  Image,
  Pressable,
  ScrollView,
  StyleSheet,
  Text,
  View,
} from "react-native";
import { useRouter } from "expo-router";

import { SoccerGame, useSoccerGames } from "@/hooks/soccer/useSoccerFdAnalytics";
import { useTheme } from "@/store/useTheme";

// ─── Helpers ──────────────────────────────────────────────────────────────────

function formatPct(value?: number | null): string {
  if (value == null) return "–";
  return `${(value * 100).toFixed(0)}%`;
}

function formatNum(value?: number | null, digits = 1): string {
  if (value == null) return "–";
  return value.toFixed(digits);
}

function formatKickoff(ts?: string | null): string {
  if (!ts) return "–";
  try {
    return new Date(ts).toLocaleString(undefined, {
      weekday: "short",
      month: "short",
      day: "numeric",
      hour: "numeric",
      minute: "2-digit",
    });
  } catch {
    return ts;
  }
}

function formatDate(dateStr?: string | null): string {
  if (!dateStr) return "";
  try {
    // event_date is already ISO date string: "2026-04-01"
    return new Date(`${dateStr}T12:00:00`).toLocaleDateString(undefined, {
      weekday: "long",
      month: "long",
      day: "numeric",
    });
  } catch {
    return dateStr;
  }
}

// ─── League badge ─────────────────────────────────────────────────────────────

function LeagueBadge({ league }: { league: string }) {
  const color = league === "EPL" ? "#A78BFA" : "#60A5FA";
  return (
    <View style={[styles.leagueBadge, { backgroundColor: `${color}20`, borderColor: `${color}40` }]}>
      <Text style={[styles.leagueBadgeText, { color }]}>{league}</Text>
    </View>
  );
}

// ─── Game card ─────────────────────────────────────────────────────────────────

function GameCard({ game, onPress }: { game: SoccerGame; onPress: () => void }) {
  const { colors } = useTheme();
  return (
    <Pressable
      style={[styles.card, { borderColor: colors.border.subtle }]}
      onPress={onPress}
    >
      {/* Header */}
      <View style={styles.cardHeader}>
        <View style={styles.teamsRow}>
          {game.home_logo ? (
            <Image source={{ uri: game.home_logo }} style={styles.teamLogo} />
          ) : null}
          <Text style={[styles.teamName, { color: colors.text.primary }]}>{game.home_team}</Text>
          <Text style={[styles.vsText, { color: colors.text.muted }]}>vs</Text>
          {game.away_logo ? (
            <Image source={{ uri: game.away_logo }} style={styles.teamLogo} />
          ) : null}
          <Text style={[styles.teamName, { color: colors.text.primary }]}>{game.away_team}</Text>
        </View>
        <LeagueBadge league={game.league} />
      </View>

      {/* Kickoff time */}
      <Text style={[styles.kickoffText, { color: colors.text.muted }]}>
        {formatKickoff(game.event_start_ts)}
      </Text>

      {/* Model signal chips */}
      <View style={styles.chipsRow}>
        {game.model_expected_total_goals != null && (
          <View style={styles.chip}>
            <Text style={styles.chipText}>
              ⚽ {formatNum(game.model_expected_total_goals)} Goals
            </Text>
          </View>
        )}
        {game.model_btts_probability != null && (
          <View style={styles.chip}>
            <Text style={styles.chipText}>
              🎯 {formatPct(game.model_btts_probability)} BTTS
            </Text>
          </View>
        )}
        {game.model_expected_corners != null && (
          <View style={styles.chip}>
            <Text style={styles.chipText}>
              📐 {formatNum(game.model_expected_corners)} Corners
            </Text>
          </View>
        )}
      </View>
    </Pressable>
  );
}

// ─── Main screen ───────────────────────────────────────────────────────────────

type Props = {
  /** Optional league filter — "EPL" or "MLS". Omit to show all leagues. */
  league?: string;
};

export function SoccerGamesScreen({ league }: Props = {}) {
  const { colors } = useTheme();
  const router = useRouter();
  const { data, loading, error, refetch } = useSoccerGames();

  const leagueUpper = league?.toUpperCase();

  // Group games by date, optionally filtered by league
  const grouped = useMemo(() => {
    const games = (data ?? []).filter(
      (g) => !leagueUpper || g.league?.toUpperCase() === leagueUpper
    );
    const map = new Map<string, SoccerGame[]>();
    for (const game of games) {
      const dateKey = game.event_date ?? "unknown";
      if (!map.has(dateKey)) map.set(dateKey, []);
      map.get(dateKey)!.push(game);
    }
    return [...map.entries()].map(([date, items]) => ({ date, items }));
  }, [data, leagueUpper]);

  const handleGamePress = useCallback(
    (game: SoccerGame) => {
      router.push({
        pathname: "/(tabs)/soccer/game/[eventId]",
        params: {
          eventId: game.fd_event_id,
          league: game.league,
          game: game.game,
          homeTeam: game.home_team,
          awayTeam: game.away_team,
          eventStartTs: game.event_start_ts ?? "",
        },
      });
    },
    [router]
  );

  return (
    <ScrollView style={styles.screen} contentContainerStyle={styles.content}>
      {/* Hero */}
      <View style={[styles.hero, { borderColor: colors.border.subtle }]}>
        <Text style={styles.eyebrow}>SOCCER ANALYTICS</Text>
        <Text style={styles.h1}>Upcoming Games</Text>
        <Text style={[styles.sub, { color: colors.text.muted }]}>
          EPL &amp; MLS — next 8 days. Tap a game for full market breakdown.
        </Text>
      </View>

      {loading && <ActivityIndicator color="#A78BFA" style={{ marginTop: 24 }} />}

      {error && (
        <Pressable
          onPress={refetch}
          style={[styles.errorBox, { borderColor: colors.border.subtle }]}
        >
          <Text style={styles.errorTitle}>Failed to load games</Text>
          <Text style={[styles.errorText, { color: colors.text.muted }]}>{error}</Text>
          <Text style={styles.errorRetry}>Tap to retry</Text>
        </Pressable>
      )}

      {grouped.map(({ date, items }) => (
        <View key={date}>
          <Text style={[styles.dateHeader, { color: colors.text.muted }]}>
            {formatDate(date)}
          </Text>
          {items.map((game) => (
            <GameCard
              key={game.fd_event_id}
              game={game}
              onPress={() => handleGamePress(game)}
            />
          ))}
        </View>
      ))}

      {!loading && !error && grouped.length === 0 && (
        <View style={[styles.emptyCard, { borderColor: colors.border.subtle }]}>
          <Text style={[styles.emptyTitle, { color: colors.text.primary }]}>No upcoming games</Text>
          <Text style={[styles.sub, { color: colors.text.muted }]}>
            Check back soon — games will appear here once scheduled.
          </Text>
        </View>
      )}
    </ScrollView>
  );
}

export default function SoccerGamesDefaultScreen() {
  return <SoccerGamesScreen />;
}

// ─── Styles ────────────────────────────────────────────────────────────────────

const styles = StyleSheet.create({
  screen: { flex: 1, backgroundColor: "#050A18" },
  content: { padding: 16, gap: 10, paddingBottom: 40 },

  hero: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 16,
    backgroundColor: "#071731",
    padding: 16,
    marginBottom: 8,
  },
  eyebrow: { color: "#90B3E9", fontSize: 11, fontWeight: "700", letterSpacing: 1 },
  h1: { color: "#E9F2FF", fontSize: 24, fontWeight: "800", marginTop: 8 },
  sub: { marginTop: 6, lineHeight: 18, fontSize: 13 },

  dateHeader: {
    fontSize: 12,
    fontWeight: "700",
    letterSpacing: 0.5,
    marginTop: 16,
    marginBottom: 6,
    paddingHorizontal: 2,
  },

  card: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 14,
    backgroundColor: "#0B1529",
    padding: 14,
    gap: 8,
    marginBottom: 8,
  },
  cardHeader: {
    flexDirection: "row",
    alignItems: "flex-start",
    justifyContent: "space-between",
    gap: 8,
  },
  teamsRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
    flexWrap: "wrap",
    flex: 1,
  },
  teamLogo: { width: 22, height: 22, borderRadius: 4 },
  teamName: { fontWeight: "800", fontSize: 15 },
  vsText: { fontSize: 12, fontWeight: "600" },

  leagueBadge: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 999,
    paddingHorizontal: 10,
    paddingVertical: 3,
  },
  leagueBadgeText: { fontSize: 10, fontWeight: "800", letterSpacing: 0.5 },

  kickoffText: { fontSize: 12 },

  chipsRow: { flexDirection: "row", flexWrap: "wrap", gap: 6 },
  chip: {
    backgroundColor: "rgba(148,163,184,0.10)",
    borderRadius: 999,
    paddingHorizontal: 10,
    paddingVertical: 4,
  },
  chipText: { color: "#CBD5E1", fontSize: 11, fontWeight: "600" },

  errorBox: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 12,
    backgroundColor: "#1F2937",
    padding: 14,
  },
  errorTitle: { color: "#FCA5A5", fontWeight: "700" },
  errorText: { marginTop: 4, fontSize: 12 },
  errorRetry: { color: "#E5E7EB", marginTop: 8, fontSize: 12 },

  emptyCard: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 14,
    backgroundColor: "#0B1529",
    padding: 20,
    alignItems: "center",
    gap: 6,
  },
  emptyTitle: { fontWeight: "700", fontSize: 14 },
});

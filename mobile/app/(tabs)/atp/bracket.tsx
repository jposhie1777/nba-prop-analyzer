import { ScrollView, StyleSheet, Text, View } from "react-native";
import { useMemo } from "react";
import { useTheme } from "@/store/useTheme";
import { useAtpTournamentBracket } from "@/hooks/atp/useAtpTournamentBracket";
import type { AtpBracketMatch, AtpBracketRound } from "@/hooks/atp/useAtpTournamentBracket";

function formatDateRange(start?: string | null, end?: string | null) {
  if (!start && !end) return "Dates TBD";
  const startDate = start ? new Date(start) : null;
  const endDate = end ? new Date(end) : null;
  const format = (value: Date | null) =>
    value
      ? value.toLocaleDateString(undefined, {
          month: "short",
          day: "numeric",
          year: "numeric",
        })
      : "TBD";
  return `${format(startDate)} - ${format(endDate)}`;
}

function formatMatchTime(value?: string | null) {
  if (!value) return "Time TBD";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "Time TBD";
  return date.toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

function isCompleted(match: AtpBracketMatch) {
  return match.status === "F" || match.status === "Finished";
}

function isQualifyingRound(round: AtpBracketRound) {
  const name = round.name.toLowerCase();
  return (
    name.includes("qualifying") ||
    name.includes("qualification") ||
    (name.startsWith("q") && /^q\d$/.test(name))
  );
}

function MatchCard({
  match,
  colors,
}: {
  match: AtpBracketMatch;
  colors: any;
}) {
  const finished = isCompleted(match);
  const winner = match.winner;
  const p1Won = finished && winner === match.player1;
  const p2Won = finished && winner === match.player2;

  return (
    <View
      style={[
        styles.matchCard,
        {
          backgroundColor: colors.surface.elevated,
          borderColor: finished ? "rgba(34,197,94,0.25)" : colors.border.subtle,
        },
      ]}
    >
      {/* Player 1 row */}
      <View style={styles.playerRow}>
        <Text
          style={[
            styles.playerText,
            {
              color: p1Won
                ? colors.accent.success
                : finished && !p1Won && winner
                  ? colors.text.muted
                  : colors.text.primary,
              fontWeight: p1Won ? "800" : "600",
            },
          ]}
          numberOfLines={1}
        >
          {match.player1}
        </Text>
        {finished && match.score && (
          <Text
            style={[
              styles.setScores,
              {
                color: p1Won ? colors.accent.success : colors.text.muted,
                fontWeight: p1Won ? "700" : "400",
              },
            ]}
          >
            {p1Won ? "W" : "L"}
          </Text>
        )}
      </View>

      {/* Divider */}
      <View style={[styles.matchDivider, { backgroundColor: colors.border.subtle }]} />

      {/* Player 2 row */}
      <View style={styles.playerRow}>
        <Text
          style={[
            styles.playerText,
            {
              color: p2Won
                ? colors.accent.success
                : finished && !p2Won && winner
                  ? colors.text.muted
                  : colors.text.primary,
              fontWeight: p2Won ? "800" : "600",
            },
          ]}
          numberOfLines={1}
        >
          {match.player2}
        </Text>
        {finished && match.score && (
          <Text
            style={[
              styles.setScores,
              {
                color: p2Won ? colors.accent.success : colors.text.muted,
                fontWeight: p2Won ? "700" : "400",
              },
            ]}
          >
            {p2Won ? "W" : "L"}
          </Text>
        )}
      </View>

      {/* Score / Time */}
      {finished && match.score ? (
        <Text style={[styles.scoreText, { color: colors.text.secondary }]}>
          {match.score}
        </Text>
      ) : (
        <Text style={[styles.matchMeta, { color: colors.text.muted }]}>
          {formatMatchTime(match.scheduled_at)}
        </Text>
      )}
    </View>
  );
}

function RoundColumn({
  round,
  colors,
}: {
  round: AtpBracketRound;
  colors: any;
}) {
  return (
    <View style={styles.roundColumn}>
      <View
        style={[
          styles.roundHeader,
          { backgroundColor: colors.surface.cardSoft },
        ]}
      >
        <Text style={[styles.roundTitle, { color: colors.text.primary }]}>
          {round.name}
        </Text>
        <Text style={[styles.roundCount, { color: colors.text.muted }]}>
          {round.matches.length} {round.matches.length === 1 ? "match" : "matches"}
        </Text>
      </View>
      {round.matches.length === 0 ? (
        <Text style={[styles.emptyText, { color: colors.text.muted }]}>
          Matches TBD
        </Text>
      ) : (
        round.matches.map((match) => (
          <MatchCard
            key={`${round.name}-${match.id ?? match.player1}-${match.player2}`}
            match={match}
            colors={colors}
          />
        ))
      )}
    </View>
  );
}

export default function AtpBracketScreen() {
  const { colors } = useTheme();
  const { data, loading, error } = useAtpTournamentBracket({
    tournamentName: "Montpellier",
    upcomingLimit: 20,
  });

  const header = useMemo(() => {
    if (!data?.tournament) {
      return {
        name: "ATP Tournament Bracket",
        surface: "Surface TBD",
        dates: "Dates TBD",
      };
    }
    return {
      name: data.tournament.name,
      surface: data.tournament.surface
        ? `${data.tournament.surface} Court`
        : "Surface TBD",
      dates: formatDateRange(
        data.tournament.start_date,
        data.tournament.end_date
      ),
    };
  }, [data]);

  const { qualifyingRounds, mainDrawRounds, stats } = useMemo(() => {
    const rounds = data?.bracket.rounds ?? [];
    const qualifying: AtpBracketRound[] = [];
    const mainDraw: AtpBracketRound[] = [];

    for (const round of rounds) {
      if (isQualifyingRound(round)) {
        qualifying.push(round);
      } else {
        mainDraw.push(round);
      }
    }

    let totalMatches = 0;
    let completedMatches = 0;
    for (const round of rounds) {
      for (const match of round.matches) {
        totalMatches++;
        if (isCompleted(match)) completedMatches++;
      }
    }

    return {
      qualifyingRounds: qualifying,
      mainDrawRounds: mainDraw,
      stats: { total: totalMatches, completed: completedMatches },
    };
  }, [data]);

  return (
    <ScrollView
      style={{ flex: 1, backgroundColor: colors.surface.screen }}
      contentContainerStyle={styles.container}
    >
      {/* Tournament Header */}
      <View
        style={[
          styles.headerCard,
          { backgroundColor: colors.surface.card, borderColor: colors.border.subtle },
        ]}
      >
        <Text style={[styles.bracketTitle, { color: colors.text.primary }]}>
          {header.name}
        </Text>
        <Text style={[styles.bracketMeta, { color: colors.text.muted }]}>
          {header.surface} {"\u2022"} {header.dates}
        </Text>
        {!loading && !error && stats.total > 0 && (
          <View style={styles.progressRow}>
            <View
              style={[
                styles.progressBar,
                { backgroundColor: colors.surface.elevated },
              ]}
            >
              <View
                style={[
                  styles.progressFill,
                  {
                    backgroundColor: colors.accent.success,
                    width: `${Math.round((stats.completed / stats.total) * 100)}%` as any,
                  },
                ]}
              />
            </View>
            <Text style={[styles.progressText, { color: colors.text.muted }]}>
              {stats.completed}/{stats.total} completed
            </Text>
          </View>
        )}
      </View>

      {loading ? (
        <Text style={[styles.statusText, { color: colors.text.muted }]}>
          Loading bracket...
        </Text>
      ) : error ? (
        <Text style={[styles.statusText, { color: colors.text.primary }]}>
          {error}
        </Text>
      ) : (
        <>
          {/* Main Draw */}
          {mainDrawRounds.length > 0 && (
            <View style={styles.section}>
              <Text style={[styles.sectionTitle, { color: colors.text.primary }]}>
                Main Draw
              </Text>
              <ScrollView
                horizontal
                showsHorizontalScrollIndicator={false}
                contentContainerStyle={styles.roundsRow}
              >
                {mainDrawRounds.map((round) => (
                  <RoundColumn key={round.name} round={round} colors={colors} />
                ))}
              </ScrollView>
            </View>
          )}

          {/* Qualifying */}
          {qualifyingRounds.length > 0 && (
            <View style={styles.section}>
              <Text style={[styles.sectionTitle, { color: colors.text.muted }]}>
                Qualifying
              </Text>
              <ScrollView
                horizontal
                showsHorizontalScrollIndicator={false}
                contentContainerStyle={styles.roundsRow}
              >
                {qualifyingRounds.map((round) => (
                  <RoundColumn key={round.name} round={round} colors={colors} />
                ))}
              </ScrollView>
            </View>
          )}
        </>
      )}
    </ScrollView>
  );
}

const styles = StyleSheet.create({
  container: {
    padding: 16,
    paddingBottom: 32,
    gap: 20,
  },
  headerCard: {
    borderRadius: 16,
    borderWidth: StyleSheet.hairlineWidth,
    padding: 16,
    gap: 8,
    alignItems: "center",
  },
  bracketTitle: {
    fontSize: 20,
    fontWeight: "800",
    textAlign: "center",
  },
  bracketMeta: {
    fontSize: 13,
    textAlign: "center",
  },
  progressRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 10,
    marginTop: 4,
    width: "100%",
  },
  progressBar: {
    flex: 1,
    height: 4,
    borderRadius: 2,
    overflow: "hidden",
  },
  progressFill: {
    height: "100%",
    borderRadius: 2,
  },
  progressText: {
    fontSize: 11,
  },
  section: {
    gap: 10,
  },
  sectionTitle: {
    fontSize: 15,
    fontWeight: "700",
    letterSpacing: 0.5,
    textTransform: "uppercase",
  },
  roundsRow: {
    paddingVertical: 4,
    gap: 12,
  },
  roundColumn: {
    width: 190,
    gap: 8,
  },
  roundHeader: {
    borderRadius: 8,
    paddingVertical: 6,
    paddingHorizontal: 10,
    gap: 2,
  },
  roundTitle: {
    fontSize: 13,
    fontWeight: "700",
  },
  roundCount: {
    fontSize: 10,
  },
  matchCard: {
    borderRadius: 10,
    borderWidth: 1,
    padding: 8,
    gap: 4,
  },
  playerRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    gap: 4,
  },
  playerText: {
    fontSize: 12,
    flex: 1,
  },
  setScores: {
    fontSize: 11,
  },
  matchDivider: {
    height: StyleSheet.hairlineWidth,
  },
  scoreText: {
    fontSize: 11,
    fontWeight: "600",
    marginTop: 2,
  },
  matchMeta: {
    fontSize: 10,
    marginTop: 2,
  },
  statusText: {
    fontSize: 13,
    textAlign: "center",
    padding: 20,
  },
  emptyText: {
    fontSize: 12,
    fontStyle: "italic",
    padding: 8,
  },
});

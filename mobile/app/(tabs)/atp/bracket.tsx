import { ScrollView, StyleSheet, Text, View } from "react-native";
import { useMemo } from "react";
import { useTheme } from "@/store/useTheme";
import { useAtpTournamentBracket } from "@/hooks/atp/useAtpTournamentBracket";

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
  if (startDate && endDate) {
    return `${format(startDate)} - ${format(endDate)}`;
  }
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

export default function AtpBracketScreen() {
  const { colors } = useTheme();
  const { data, loading, error } = useAtpTournamentBracket({
    tournamentName: "Montpellier",
    upcomingLimit: 6,
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

  return (
    <ScrollView
      style={{ flex: 1, backgroundColor: colors.surface.screen }}
      contentContainerStyle={styles.container}
    >
      <View
        style={[
          styles.bracketCard,
          { backgroundColor: colors.surface.card, borderColor: colors.border.subtle },
        ]}
      >
        <Text style={[styles.bracketTitle, { color: colors.text.primary }]}>
          {header.name}
        </Text>
        <Text style={[styles.bracketMeta, { color: colors.text.muted }]}>
          {header.surface} • {header.dates}
        </Text>

        {loading ? (
          <Text style={[styles.statusText, { color: colors.text.muted }]}>
            Loading bracket...
          </Text>
        ) : error ? (
          <Text style={[styles.statusText, { color: colors.text.error }]}>
            {error}
          </Text>
        ) : (
          <ScrollView
            horizontal
            showsHorizontalScrollIndicator={false}
            contentContainerStyle={styles.roundsRow}
          >
            {(data?.bracket.rounds ?? []).map((round) => (
              <View key={round.name} style={styles.roundColumn}>
                <Text
                  style={[styles.roundTitle, { color: colors.text.primary }]}
                >
                  {round.name}
                </Text>
                {round.matches.length === 0 ? (
                  <Text style={[styles.emptyText, { color: colors.text.muted }]}>
                    Matches TBD
                  </Text>
                ) : (
                  round.matches.map((match) => (
                    <View
                      key={`${round.name}-${match.id ?? match.player1}`}
                      style={[
                        styles.matchCard,
                        {
                          backgroundColor: colors.surface.elevated,
                          borderColor: colors.border.subtle,
                        },
                      ]}
                    >
                      <Text
                        style={[
                          styles.playerText,
                          { color: colors.text.primary },
                        ]}
                      >
                        {match.player1}
                      </Text>
                      <Text
                        style={[
                          styles.playerText,
                          { color: colors.text.primary },
                        ]}
                      >
                        {match.player2}
                      </Text>
                      <Text
                        style={[
                          styles.matchMeta,
                          { color: colors.text.muted },
                        ]}
                      >
                        {formatMatchTime(match.scheduled_at)}
                      </Text>
                    </View>
                  ))
                )}
              </View>
            ))}
          </ScrollView>
        )}
      </View>

      <View style={styles.section}>
        <Text style={[styles.sectionTitle, { color: colors.text.primary }]}>
          Upcoming Matches
        </Text>
        {loading ? (
          <Text style={[styles.statusText, { color: colors.text.muted }]}>
            Loading upcoming matches...
          </Text>
        ) : error ? (
          <Text style={[styles.statusText, { color: colors.text.error }]}>
            {error}
          </Text>
        ) : (data?.upcoming_matches ?? []).length === 0 ? (
          <Text style={[styles.statusText, { color: colors.text.muted }]}>
            No upcoming matches found.
          </Text>
        ) : (
          (data?.upcoming_matches ?? []).map((match) => (
            <View
              key={`upcoming-${match.id ?? match.player1}`}
              style={[
                styles.upcomingCard,
                {
                  backgroundColor: colors.surface.card,
                  borderColor: colors.border.subtle,
                },
              ]}
            >
              <Text style={[styles.playerText, { color: colors.text.primary }]}>
                {match.player1} vs {match.player2}
              </Text>
              <Text style={[styles.matchMeta, { color: colors.text.muted }]}>
                {match.round} • {formatMatchTime(match.scheduled_at)}
              </Text>
            </View>
          ))
        )}
      </View>
    </ScrollView>
  );
}

const styles = StyleSheet.create({
  container: {
    padding: 16,
    paddingBottom: 32,
    gap: 16,
  },
  bracketCard: {
    borderRadius: 16,
    borderWidth: StyleSheet.hairlineWidth,
    padding: 16,
    gap: 8,
  },
  bracketTitle: {
    fontSize: 18,
    fontWeight: "800",
    textAlign: "center",
  },
  bracketMeta: {
    fontSize: 12,
    textAlign: "center",
  },
  roundsRow: {
    paddingVertical: 8,
    gap: 12,
  },
  roundColumn: {
    width: 180,
    gap: 10,
  },
  roundTitle: {
    fontSize: 14,
    fontWeight: "700",
  },
  matchCard: {
    borderRadius: 12,
    borderWidth: StyleSheet.hairlineWidth,
    padding: 10,
    gap: 6,
  },
  playerText: {
    fontSize: 13,
    fontWeight: "600",
  },
  matchMeta: {
    fontSize: 11,
  },
  statusText: {
    fontSize: 12,
  },
  emptyText: {
    fontSize: 12,
    fontStyle: "italic",
  },
  section: {
    gap: 10,
  },
  sectionTitle: {
    fontSize: 16,
    fontWeight: "700",
  },
  upcomingCard: {
    borderRadius: 12,
    borderWidth: StyleSheet.hairlineWidth,
    padding: 12,
    gap: 4,
  },
});

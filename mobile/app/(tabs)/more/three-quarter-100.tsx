import {
  ActivityIndicator,
  Pressable,
  ScrollView,
  StyleSheet,
  Text,
  View,
} from "react-native";
import { Stack } from "expo-router";

import { useTheme } from "@/store/useTheme";
import { useThreeQuarter100 } from "@/hooks/useThreeQuarter100";
import { ThreeQ100Game, ThreeQ100Team } from "@/lib/threeQuarter100";

const formatPercent = (value?: number | null) => {
  if (value === null || value === undefined) return "—";
  return `${Math.round(value * 100)}%`;
};

const formatNumber = (value?: number | null, digits = 1) => {
  if (value === null || value === undefined) return "—";
  return value.toFixed(digits);
};

function Metric({
  label,
  value,
}: {
  label: string;
  value: string;
}) {
  const { colors } = useTheme();

  return (
    <View style={styles.metric}>
      <Text style={[styles.metricLabel, { color: colors.text.muted }]}>
        {label}
      </Text>
      <Text style={[styles.metricValue, { color: colors.text.primary }]}>
        {value}
      </Text>
    </View>
  );
}

function TeamRow({
  team,
  highlight,
}: {
  team: ThreeQ100Team;
  highlight: boolean;
}) {
  const { colors } = useTheme();
  const sample = `${team.games_played ?? "—"}/${team.games_defended ?? "—"}`;

  return (
    <View
      style={[
        styles.teamCard,
        {
          backgroundColor: colors.surface.cardSoft,
          borderColor: highlight ? colors.accent.primary : colors.border.subtle,
        },
      ]}
    >
      <View style={styles.teamHeader}>
        <View>
          <Text style={[styles.teamName, { color: colors.text.primary }]}>
            {team.team_abbr}
          </Text>
          <Text style={[styles.teamMeta, { color: colors.text.muted }]}>
            {team.side === "HOME" ? "Home" : "Away"} vs {team.opponent_abbr}
          </Text>
        </View>
        <View
          style={[
            styles.hitPill,
            {
              backgroundColor: highlight
                ? colors.glow.primary
                : colors.surface.card,
              borderColor: colors.border.subtle,
            },
          ]}
        >
          <Text style={[styles.hitPillLabel, { color: colors.text.muted }]}>
            Likely 100 in 3Q
          </Text>
          <Text style={[styles.hitPillValue, { color: colors.text.primary }]}>
            {formatPercent(team.predicted_hit_rate)}
          </Text>
        </View>
      </View>

      <View style={styles.metricsRow}>
        <Metric label="Team 3Q 100%" value={formatPercent(team.hit_100_rate)} />
        <Metric
          label="Opp 3Q 100%"
          value={formatPercent(team.allow_100_rate)}
        />
        <Metric
          label="Proj 3Q pts"
          value={formatNumber(team.predicted_3q_points)}
        />
      </View>

      <View style={styles.metricsRow}>
        <Metric
          label="Avg 3Q pts"
          value={formatNumber(team.avg_3q_points)}
        />
        <Metric
          label="Opp 3Q allowed"
          value={formatNumber(team.avg_3q_allowed)}
        />
        <Metric label="Samples" value={sample} />
      </View>
    </View>
  );
}

function GameCard({ game }: { game: ThreeQ100Game }) {
  const { colors } = useTheme();
  const bestTeam = game.teams.reduce<ThreeQ100Team | null>(
    (best, team) => {
      const bestValue = best?.predicted_hit_rate ?? -1;
      const teamValue = team.predicted_hit_rate ?? -1;
      return teamValue > bestValue ? team : best;
    },
    null
  );

  return (
    <View
      style={[
        styles.card,
        {
          backgroundColor: colors.surface.card,
          borderColor: colors.border.subtle,
        },
      ]}
    >
      <View style={styles.cardHeader}>
        <Text style={[styles.cardTitle, { color: colors.text.primary }]}>
          {game.away_team_abbr} @ {game.home_team_abbr}
        </Text>
        <Text style={[styles.cardDate, { color: colors.text.muted }]}>
          {game.start_time_est
            ? new Date(game.start_time_est).toLocaleString()
            : "TBD"}
        </Text>
      </View>

      {game.teams.map((team) => (
        <TeamRow
          key={`${game.game_id}-${team.team_abbr}-${team.side}`}
          team={team}
          highlight={Boolean(
            bestTeam?.team_abbr === team.team_abbr &&
              bestTeam?.predicted_hit_rate != null
          )}
        />
      ))}
    </View>
  );
}

export default function ThreeQuarter100Screen() {
  const { colors } = useTheme();
  const { data, loading, error, refresh } = useThreeQuarter100();

  const games = data?.games ?? [];
  const updatedAt = data?.generated_at;

  return (
    <>
      <Stack.Screen
        options={{
          title: "100 in 3Q",
          headerStyle: { backgroundColor: colors.surface.screen },
          headerTintColor: colors.text.primary,
        }}
      />

      <ScrollView
        style={{ flex: 1, backgroundColor: colors.surface.screen }}
        contentContainerStyle={styles.container}
      >
        <View style={styles.headerRow}>
          <View style={{ flex: 1 }}>
            <Text style={[styles.pageTitle, { color: colors.text.primary }]}>
              100 in 3Q
            </Text>
            <Text
              style={[styles.pageSubtitle, { color: colors.text.muted }]}
            >
              Team history + opponent points allowed through 3 quarters
            </Text>
            {updatedAt && (
              <Text
                style={[styles.pageSubtitle, { color: colors.text.muted }]}
              >
                Updated {new Date(updatedAt).toLocaleString()}
              </Text>
            )}
          </View>

          <Pressable
            onPress={refresh}
            style={[
              styles.refreshButton,
              {
                backgroundColor: colors.surface.card,
                borderColor: colors.border.subtle,
              },
            ]}
            disabled={loading}
          >
            <Text
              style={[styles.refreshText, { color: colors.text.primary }]}
            >
              Refresh
            </Text>
          </Pressable>
        </View>

        {loading && (
          <View style={styles.centered}>
            <ActivityIndicator color={colors.accent.primary} />
          </View>
        )}

        {error && (
          <View style={styles.centered}>
            <Text style={{ color: colors.text.danger }}>
              {error}
            </Text>
          </View>
        )}

        {!loading && !error && games.length === 0 && (
          <View style={styles.centered}>
            <Text style={{ color: colors.text.muted }}>
              No games on the schedule tonight.
            </Text>
          </View>
        )}

        {!loading &&
          !error &&
          games.map((game) => (
            <GameCard key={game.game_id} game={game} />
          ))}
      </ScrollView>
    </>
  );
}

const styles = StyleSheet.create({
  container: {
    padding: 12,
    paddingBottom: 24,
    gap: 12,
  },
  headerRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 12,
  },
  pageTitle: {
    fontSize: 20,
    fontWeight: "900",
  },
  pageSubtitle: {
    fontSize: 12,
    marginTop: 4,
    fontWeight: "600",
  },
  refreshButton: {
    paddingHorizontal: 12,
    paddingVertical: 8,
    borderRadius: 10,
    borderWidth: 1,
  },
  refreshText: {
    fontSize: 12,
    fontWeight: "700",
  },
  centered: {
    alignItems: "center",
    paddingVertical: 20,
  },
  card: {
    padding: 12,
    borderRadius: 16,
    borderWidth: 1,
    gap: 12,
  },
  cardHeader: {
    flexDirection: "row",
    justifyContent: "space-between",
    gap: 12,
  },
  cardTitle: {
    fontSize: 16,
    fontWeight: "800",
  },
  cardDate: {
    fontSize: 12,
    fontWeight: "600",
  },
  teamCard: {
    borderRadius: 12,
    borderWidth: 1,
    padding: 10,
    gap: 8,
  },
  teamHeader: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    gap: 12,
  },
  teamName: {
    fontSize: 15,
    fontWeight: "800",
  },
  teamMeta: {
    fontSize: 11,
    fontWeight: "600",
    marginTop: 2,
  },
  hitPill: {
    paddingHorizontal: 10,
    paddingVertical: 6,
    borderRadius: 10,
    borderWidth: 1,
    alignItems: "flex-end",
  },
  hitPillLabel: {
    fontSize: 10,
    fontWeight: "600",
  },
  hitPillValue: {
    fontSize: 14,
    fontWeight: "800",
    marginTop: 2,
  },
  metricsRow: {
    flexDirection: "row",
    gap: 12,
  },
  metric: {
    flex: 1,
  },
  metricLabel: {
    fontSize: 10,
    fontWeight: "600",
  },
  metricValue: {
    fontSize: 13,
    fontWeight: "700",
    marginTop: 2,
  },
});

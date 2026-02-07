// app/(tabs)/more/game-environment.tsx
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
import { useGameEnvironment } from "@/hooks/useGameEnvironment";
import { GameEnvironment } from "@/lib/gameEnvironment";

// ─── Helpers ────────────────────────────────────

const formatNum = (v?: number | null, digits = 1) => {
  if (v === null || v === undefined) return "--";
  return v.toFixed(digits);
};

const tierEmoji = (tier: string) => {
  switch (tier) {
    case "SHOOTOUT":
      return "SHOOTOUT";
    case "HIGH":
      return "HIGH";
    case "ABOVE_AVG":
      return "ABOVE AVG";
    case "AVERAGE":
      return "AVERAGE";
    case "BELOW_AVG":
      return "BELOW AVG";
    case "GRIND":
      return "GRIND";
    default:
      return "UNKNOWN";
  }
};

const tierColor = (tier: string, colors: any) => {
  switch (tier) {
    case "SHOOTOUT":
    case "HIGH":
      return colors.accent.success;
    case "ABOVE_AVG":
      return colors.accent.info;
    case "AVERAGE":
      return colors.accent.warning;
    case "BELOW_AVG":
      return colors.accent.warning;
    case "GRIND":
      return colors.accent.danger;
    default:
      return colors.text.muted;
  }
};

const blowoutColor = (level: string, colors: any) => {
  switch (level) {
    case "high":
      return colors.accent.danger;
    case "moderate":
      return colors.accent.warning;
    case "low":
      return colors.accent.info;
    case "minimal":
      return colors.accent.success;
    default:
      return colors.text.muted;
  }
};

// ─── Metric Component ────────────────────────────

function Metric({
  label,
  value,
  color,
}: {
  label: string;
  value: string;
  color?: string;
}) {
  const { colors } = useTheme();

  return (
    <View style={styles.metric}>
      <Text style={[styles.metricLabel, { color: colors.text.muted }]}>
        {label}
      </Text>
      <Text
        style={[
          styles.metricValue,
          { color: color ?? colors.text.primary },
        ]}
      >
        {value}
      </Text>
    </View>
  );
}

// ─── Team Rest Badge ─────────────────────────────

function RestBadge({
  teamAbbr,
  restDays,
  isB2B,
}: {
  teamAbbr: string;
  restDays: number;
  isB2B: boolean;
}) {
  const { colors } = useTheme();

  const restColor = isB2B
    ? colors.accent.danger
    : restDays >= 3
      ? colors.accent.success
      : colors.text.secondary;

  const restLabel = isB2B
    ? "B2B"
    : restDays === 1
      ? "1 day rest"
      : `${restDays} days rest`;

  return (
    <View
      style={[
        styles.restBadge,
        {
          backgroundColor: restColor + "15",
          borderColor: restColor + "40",
        },
      ]}
    >
      <Text style={[styles.restTeam, { color: colors.text.primary }]}>
        {teamAbbr}
      </Text>
      <Text style={[styles.restLabel, { color: restColor }]}>
        {restLabel}
      </Text>
    </View>
  );
}

// ─── Pace Bar ────────────────────────────────────

function PaceBar({
  label,
  pace,
  paceLabel,
  maxPace = 110,
  minPace = 90,
}: {
  label: string;
  pace: number | null;
  paceLabel: string;
  maxPace?: number;
  minPace?: number;
}) {
  const { colors } = useTheme();

  if (pace === null) return null;

  const pct = Math.max(
    0,
    Math.min(100, ((pace - minPace) / (maxPace - minPace)) * 100),
  );

  const barColor =
    pace >= 102
      ? colors.accent.success
      : pace >= 98
        ? colors.accent.info
        : pace >= 95
          ? colors.accent.warning
          : colors.accent.danger;

  return (
    <View style={styles.paceBarContainer}>
      <View style={styles.paceBarHeader}>
        <Text style={[styles.paceBarLabel, { color: colors.text.muted }]}>
          {label}
        </Text>
        <Text style={[styles.paceBarValue, { color: colors.text.primary }]}>
          {formatNum(pace)} &middot; {paceLabel}
        </Text>
      </View>
      <View
        style={[
          styles.paceBarTrack,
          { backgroundColor: colors.surface.cardSoft },
        ]}
      >
        <View
          style={[
            styles.paceBarFill,
            { width: `${pct}%`, backgroundColor: barColor },
          ]}
        />
      </View>
    </View>
  );
}

// ─── Game Card ───────────────────────────────────

function EnvironmentCard({ game }: { game: GameEnvironment }) {
  const { colors } = useTheme();
  const tColor = tierColor(game.environment_tier, colors);
  const bColor = blowoutColor(game.blowout_risk.level, colors);

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
      {/* Header */}
      <View style={styles.cardHeader}>
        <View>
          <Text style={[styles.cardTitle, { color: colors.text.primary }]}>
            {game.away_team_abbr} @ {game.home_team_abbr}
          </Text>
          <Text style={[styles.cardDate, { color: colors.text.muted }]}>
            {game.start_time_est
              ? new Date(game.start_time_est).toLocaleString(undefined, {
                  hour: "numeric",
                  minute: "2-digit",
                })
              : "TBD"}
          </Text>
        </View>

        {/* Environment tier badge */}
        <View
          style={[
            styles.tierBadge,
            {
              backgroundColor: tColor + "18",
              borderColor: tColor + "50",
            },
          ]}
        >
          <Text style={[styles.tierLabel, { color: tColor }]}>
            {tierEmoji(game.environment_tier)}
          </Text>
        </View>
      </View>

      {/* Vegas Lines Row */}
      <View style={styles.metricsRow}>
        <Metric
          label="Vegas Total"
          value={game.vegas_total ? formatNum(game.vegas_total, 1) : "--"}
          color={tColor}
        />
        <Metric
          label="Spread"
          value={
            game.spread_home != null
              ? `${game.home_team_abbr} ${game.spread_home > 0 ? "+" : ""}${formatNum(game.spread_home, 1)}`
              : "--"
          }
        />
        <Metric
          label="Proj Total"
          value={formatNum(game.projected_total, 1)}
        />
      </View>

      {/* Pace Section */}
      <View
        style={[
          styles.paceSection,
          {
            backgroundColor: colors.surface.cardSoft,
            borderColor: colors.border.subtle,
          },
        ]}
      >
        <View style={styles.paceSectionHeader}>
          <Text
            style={[styles.paceSectionTitle, { color: colors.text.primary }]}
          >
            Pace Profile
          </Text>
          {game.combined_pace && (
            <Text
              style={[styles.combinedPace, { color: colors.text.secondary }]}
            >
              Combined: {formatNum(game.combined_pace)}
            </Text>
          )}
        </View>

        <PaceBar
          label={game.home_team_abbr}
          pace={game.home_pace}
          paceLabel={game.home_pace_label}
        />
        <PaceBar
          label={game.away_team_abbr}
          pace={game.away_pace}
          paceLabel={game.away_pace_label}
        />
      </View>

      {/* Scoring + Blowout Row */}
      <View style={styles.metricsRow}>
        <Metric
          label={`${game.home_team_abbr} PPG`}
          value={formatNum(game.home_pts_avg, 1)}
        />
        <Metric
          label={`${game.away_team_abbr} PPG`}
          value={formatNum(game.away_pts_avg, 1)}
        />
        <View style={styles.metric}>
          <Text style={[styles.metricLabel, { color: colors.text.muted }]}>
            Blowout Risk
          </Text>
          <Text
            style={[styles.metricValue, { color: bColor }]}
          >
            {game.blowout_risk.label}
          </Text>
        </View>
      </View>

      {/* Rest Badges */}
      <View style={styles.restRow}>
        <RestBadge
          teamAbbr={game.home_team_abbr}
          restDays={game.home_rest_days}
          isB2B={game.home_b2b}
        />
        <RestBadge
          teamAbbr={game.away_team_abbr}
          restDays={game.away_rest_days}
          isB2B={game.away_b2b}
        />
      </View>

      {/* Stat Impacts */}
      <View
        style={[
          styles.impactsSection,
          {
            backgroundColor: colors.surface.cardSoft,
            borderColor: colors.border.subtle,
          },
        ]}
      >
        <Text
          style={[styles.impactsTitle, { color: colors.text.primary }]}
        >
          Prop Impacts
        </Text>
        {game.stat_impacts.map((impact, idx) => (
          <View key={idx} style={styles.impactRow}>
            <View
              style={[
                styles.impactDot,
                { backgroundColor: colors.accent.primary },
              ]}
            />
            <Text
              style={[styles.impactText, { color: colors.text.secondary }]}
            >
              {impact}
            </Text>
          </View>
        ))}
      </View>
    </View>
  );
}

// ─── Main Screen ─────────────────────────────────

export default function GameEnvironmentScreen() {
  const { colors } = useTheme();
  const { data, loading, error, refresh } = useGameEnvironment();

  const games = data?.games ?? [];

  return (
    <>
      <Stack.Screen
        options={{
          title: "Game Environment",
          headerStyle: { backgroundColor: colors.surface.screen },
          headerTintColor: colors.text.primary,
        }}
      />

      <ScrollView
        style={{ flex: 1, backgroundColor: colors.surface.screen }}
        contentContainerStyle={styles.container}
      >
        {/* Header */}
        <View style={styles.headerRow}>
          <View style={{ flex: 1 }}>
            <Text style={[styles.pageTitle, { color: colors.text.primary }]}>
              Game Environment
            </Text>
            <Text style={[styles.pageSubtitle, { color: colors.text.muted }]}>
              Pace, totals, rest &amp; scoring context for tonight
            </Text>
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

        {/* Legend */}
        <ScrollView
          horizontal
          showsHorizontalScrollIndicator={false}
          contentContainerStyle={styles.legendRow}
        >
          {[
            { label: "SHOOTOUT", color: colors.accent.success },
            { label: "HIGH", color: colors.accent.success },
            { label: "ABOVE AVG", color: colors.accent.info },
            { label: "AVERAGE", color: colors.accent.warning },
            { label: "GRIND", color: colors.accent.danger },
          ].map((item) => (
            <View key={item.label} style={styles.legendItem}>
              <View
                style={[styles.legendDot, { backgroundColor: item.color }]}
              />
              <Text
                style={[styles.legendText, { color: colors.text.muted }]}
              >
                {item.label}
              </Text>
            </View>
          ))}
        </ScrollView>

        {/* States */}
        {loading && (
          <View style={styles.centered}>
            <ActivityIndicator color={colors.accent.primary} />
          </View>
        )}

        {error && (
          <View style={styles.centered}>
            <Text style={{ color: colors.accent.danger }}>{error}</Text>
          </View>
        )}

        {!loading && !error && games.length === 0 && (
          <View style={styles.centered}>
            <Text style={{ color: colors.text.muted }}>
              No games on the schedule tonight.
            </Text>
          </View>
        )}

        {/* Game cards */}
        {!loading &&
          !error &&
          games.map((game) => (
            <EnvironmentCard key={game.game_id} game={game} />
          ))}
      </ScrollView>
    </>
  );
}

// ─── Styles ──────────────────────────────────────

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

  // Legend
  legendRow: {
    flexDirection: "row",
    gap: 14,
    paddingVertical: 4,
  },
  legendItem: {
    flexDirection: "row",
    alignItems: "center",
    gap: 5,
  },
  legendDot: {
    width: 8,
    height: 8,
    borderRadius: 4,
  },
  legendText: {
    fontSize: 10,
    fontWeight: "700",
  },

  // Card
  card: {
    padding: 12,
    borderRadius: 16,
    borderWidth: 1,
    gap: 10,
  },
  cardHeader: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "flex-start",
    gap: 12,
  },
  cardTitle: {
    fontSize: 16,
    fontWeight: "800",
  },
  cardDate: {
    fontSize: 12,
    fontWeight: "600",
    marginTop: 2,
  },

  // Tier badge
  tierBadge: {
    paddingHorizontal: 10,
    paddingVertical: 6,
    borderRadius: 8,
    borderWidth: 1,
  },
  tierLabel: {
    fontSize: 11,
    fontWeight: "800",
    letterSpacing: 0.5,
  },

  // Metrics
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

  // Pace section
  paceSection: {
    borderRadius: 10,
    borderWidth: 1,
    padding: 10,
    gap: 8,
  },
  paceSectionHeader: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
  },
  paceSectionTitle: {
    fontSize: 13,
    fontWeight: "800",
  },
  combinedPace: {
    fontSize: 11,
    fontWeight: "600",
  },

  // Pace bar
  paceBarContainer: {
    gap: 4,
  },
  paceBarHeader: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
  },
  paceBarLabel: {
    fontSize: 11,
    fontWeight: "700",
  },
  paceBarValue: {
    fontSize: 11,
    fontWeight: "600",
  },
  paceBarTrack: {
    height: 6,
    borderRadius: 3,
    overflow: "hidden",
  },
  paceBarFill: {
    height: "100%",
    borderRadius: 3,
  },

  // Rest
  restRow: {
    flexDirection: "row",
    gap: 10,
  },
  restBadge: {
    flex: 1,
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    paddingHorizontal: 10,
    paddingVertical: 7,
    borderRadius: 8,
    borderWidth: 1,
  },
  restTeam: {
    fontSize: 13,
    fontWeight: "800",
  },
  restLabel: {
    fontSize: 11,
    fontWeight: "700",
  },

  // Impacts
  impactsSection: {
    borderRadius: 10,
    borderWidth: 1,
    padding: 10,
    gap: 6,
  },
  impactsTitle: {
    fontSize: 13,
    fontWeight: "800",
    marginBottom: 2,
  },
  impactRow: {
    flexDirection: "row",
    alignItems: "flex-start",
    gap: 8,
  },
  impactDot: {
    width: 5,
    height: 5,
    borderRadius: 3,
    marginTop: 5,
  },
  impactText: {
    fontSize: 12,
    fontWeight: "500",
    flex: 1,
    lineHeight: 17,
  },
});

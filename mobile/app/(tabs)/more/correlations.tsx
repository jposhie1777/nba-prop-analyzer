// app/(tabs)/more/correlations.tsx
import {
  ActivityIndicator,
  Pressable,
  ScrollView,
  StyleSheet,
  Text,
  View,
} from "react-native";
import { Stack } from "expo-router";
import { useMemo, useState } from "react";

import { useTheme } from "@/store/useTheme";
import { useCorrelations } from "@/hooks/useCorrelations";
import { Correlation, CorrelationGame } from "@/lib/correlations";

// ─── Helpers ────────────────────────────────────

const strengthColor = (
  strength: string,
  colors: any,
): string => {
  switch (strength) {
    case "strong":
      return colors.accent.success;
    case "moderate":
      return colors.accent.warning;
    case "weak":
      return colors.accent.info;
    default:
      return colors.text.muted;
  }
};

const formatPct = (v?: number | null) => {
  if (v === null || v === undefined) return "--";
  return `${Math.round(v * 100)}%`;
};

const formatCorr = (v: number) => {
  const sign = v >= 0 ? "+" : "";
  return `${sign}${(v * 100).toFixed(0)}%`;
};

const metricLabel = (metric: string) => {
  switch (metric) {
    case "usage_percentage":
      return "Usage";
    case "offensive_rating":
      return "Efficiency";
    case "assist_ratio":
      return "Assists";
    case "rebound_percentage":
      return "Rebounding";
    case "pie":
      return "Impact";
    case "pace":
      return "Pace";
    default:
      return metric;
  }
};

const marketLabel = (m: string) => {
  const labels: Record<string, string> = {
    pts: "PTS",
    reb: "REB",
    ast: "AST",
    "3pm": "3PM",
    pra: "PRA",
    pa: "P+A",
    pr: "P+R",
    ra: "R+A",
    fga: "FGA",
  };
  return labels[m] ?? m.toUpperCase();
};

// ─── Filter Chips ────────────────────────────────

type FilterOption = "all" | "strong" | "moderate" | "positive" | "negative";

function FilterChip({
  label,
  active,
  onPress,
}: {
  label: string;
  active: boolean;
  onPress: () => void;
}) {
  const { colors } = useTheme();

  return (
    <Pressable
      onPress={onPress}
      style={[
        styles.chip,
        {
          backgroundColor: active
            ? colors.accent.primary
            : colors.surface.cardSoft,
          borderColor: active
            ? colors.accent.primary
            : colors.border.subtle,
        },
      ]}
    >
      <Text
        style={[
          styles.chipText,
          { color: active ? "#FFFFFF" : colors.text.secondary },
        ]}
      >
        {label}
      </Text>
    </Pressable>
  );
}

// ─── Correlation Card ────────────────────────────

function CorrelationCard({ corr }: { corr: Correlation }) {
  const { colors } = useTheme();
  const sColor = strengthColor(corr.correlation_strength, colors);

  return (
    <View
      style={[
        styles.corrCard,
        {
          backgroundColor: colors.surface.cardSoft,
          borderColor: colors.border.subtle,
        },
      ]}
    >
      {/* Header row: Player pair + strength badge */}
      <View style={styles.corrHeader}>
        <View style={{ flex: 1 }}>
          <Text style={[styles.corrPlayers, { color: colors.text.primary }]}>
            {corr.player_a_name} + {corr.player_b_name}
          </Text>
          <Text style={[styles.corrTeam, { color: colors.text.muted }]}>
            {corr.team_abbr} &middot; {corr.shared_games} shared games
          </Text>
        </View>

        <View
          style={[
            styles.strengthBadge,
            { backgroundColor: sColor + "20", borderColor: sColor },
          ]}
        >
          <Text style={[styles.strengthLabel, { color: sColor }]}>
            {corr.correlation_strength.toUpperCase()}
          </Text>
          <Text style={[styles.strengthValue, { color: sColor }]}>
            {formatCorr(corr.correlation_coefficient)}
          </Text>
        </View>
      </View>

      {/* Insight */}
      <Text style={[styles.insight, { color: colors.text.secondary }]}>
        {corr.insight}
      </Text>

      {/* Metrics row */}
      <View style={styles.metricsRow}>
        <View style={styles.metric}>
          <Text style={[styles.metricLabel, { color: colors.text.muted }]}>
            Metric
          </Text>
          <Text style={[styles.metricValue, { color: colors.text.primary }]}>
            {metricLabel(corr.correlation_metric)}
          </Text>
        </View>

        <View style={styles.metric}>
          <Text style={[styles.metricLabel, { color: colors.text.muted }]}>
            Direction
          </Text>
          <Text
            style={[
              styles.metricValue,
              {
                color:
                  corr.direction === "positive"
                    ? colors.accent.success
                    : colors.accent.danger,
              },
            ]}
          >
            {corr.direction === "positive" ? "Positive" : "Negative"}
          </Text>
        </View>

        <View style={styles.metric}>
          <Text style={[styles.metricLabel, { color: colors.text.muted }]}>
            Both Over
          </Text>
          <Text style={[styles.metricValue, { color: colors.text.primary }]}>
            {formatPct(corr.both_over_rate)}
          </Text>
        </View>
      </View>

      {/* Active markets */}
      <View style={styles.marketsRow}>
        {corr.relevant_markets.map((m) => (
          <View
            key={m}
            style={[
              styles.marketTag,
              {
                backgroundColor: colors.accent.primary + "15",
                borderColor: colors.accent.primary + "40",
              },
            ]}
          >
            <Text
              style={[styles.marketTagText, { color: colors.accent.primary }]}
            >
              {marketLabel(m)}
            </Text>
          </View>
        ))}
      </View>
    </View>
  );
}

// ─── Game Card ───────────────────────────────────

function GameCard({ game }: { game: CorrelationGame }) {
  const { colors } = useTheme();

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
        <Text style={[styles.cardCount, { color: colors.text.muted }]}>
          {game.correlations.length} pair{game.correlations.length !== 1 ? "s" : ""}
        </Text>
      </View>

      {game.correlations.map((corr, idx) => (
        <CorrelationCard
          key={`${corr.player_a_id}-${corr.player_b_id}-${idx}`}
          corr={corr}
        />
      ))}
    </View>
  );
}

// ─── Main Screen ─────────────────────────────────

export default function CorrelationsScreen() {
  const { colors } = useTheme();
  const { data, loading, error, refresh } = useCorrelations();
  const [filter, setFilter] = useState<FilterOption>("all");

  const games = data?.games ?? [];

  const filteredGames = useMemo(() => {
    if (filter === "all") return games;

    return games
      .map((game) => ({
        ...game,
        correlations: game.correlations.filter((c) => {
          if (filter === "strong") return c.correlation_strength === "strong";
          if (filter === "moderate")
            return (
              c.correlation_strength === "strong" ||
              c.correlation_strength === "moderate"
            );
          if (filter === "positive") return c.direction === "positive";
          if (filter === "negative") return c.direction === "negative";
          return true;
        }),
      }))
      .filter((game) => game.correlations.length > 0);
  }, [games, filter]);

  const totalCorrelations = filteredGames.reduce(
    (sum, g) => sum + g.correlations.length,
    0,
  );

  return (
    <>
      <Stack.Screen
        options={{
          title: "Correlations",
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
              Prop Correlations
            </Text>
            <Text style={[styles.pageSubtitle, { color: colors.text.muted }]}>
              Teammate pairs that tend to hit together
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

        {/* Filter chips */}
        <ScrollView
          horizontal
          showsHorizontalScrollIndicator={false}
          contentContainerStyle={styles.chipRow}
        >
          <FilterChip
            label="All"
            active={filter === "all"}
            onPress={() => setFilter("all")}
          />
          <FilterChip
            label="Strong"
            active={filter === "strong"}
            onPress={() => setFilter("strong")}
          />
          <FilterChip
            label="Moderate+"
            active={filter === "moderate"}
            onPress={() => setFilter("moderate")}
          />
          <FilterChip
            label="Positive"
            active={filter === "positive"}
            onPress={() => setFilter("positive")}
          />
          <FilterChip
            label="Negative"
            active={filter === "negative"}
            onPress={() => setFilter("negative")}
          />
        </ScrollView>

        {/* Summary pill */}
        {!loading && !error && totalCorrelations > 0 && (
          <View
            style={[
              styles.summaryPill,
              {
                backgroundColor: colors.accent.primary + "15",
                borderColor: colors.accent.primary + "40",
              },
            ]}
          >
            <Text
              style={[styles.summaryText, { color: colors.accent.primary }]}
            >
              {totalCorrelations} correlation{totalCorrelations !== 1 ? "s" : ""}{" "}
              across {filteredGames.length} game{filteredGames.length !== 1 ? "s" : ""}
            </Text>
          </View>
        )}

        {/* States */}
        {loading && (
          <View style={styles.centered}>
            <ActivityIndicator color={colors.accent.primary} />
            <Text style={[styles.loadingText, { color: colors.text.muted }]}>
              Computing correlations...
            </Text>
          </View>
        )}

        {error && (
          <View style={styles.centered}>
            <Text style={{ color: colors.accent.danger }}>{error}</Text>
          </View>
        )}

        {!loading && !error && totalCorrelations === 0 && (
          <View style={styles.centered}>
            <Text style={{ color: colors.text.muted }}>
              No correlations found for tonight's games.
            </Text>
          </View>
        )}

        {/* Game cards */}
        {!loading &&
          !error &&
          filteredGames.map((game) => (
            <GameCard key={game.game_id ?? "unknown"} game={game} />
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
    gap: 8,
  },
  loadingText: {
    fontSize: 12,
    fontWeight: "600",
    marginTop: 4,
  },

  // Filter chips
  chipRow: {
    flexDirection: "row",
    gap: 8,
    paddingVertical: 4,
  },
  chip: {
    paddingHorizontal: 14,
    paddingVertical: 7,
    borderRadius: 20,
    borderWidth: 1,
  },
  chipText: {
    fontSize: 12,
    fontWeight: "700",
  },

  // Summary
  summaryPill: {
    paddingHorizontal: 14,
    paddingVertical: 8,
    borderRadius: 10,
    borderWidth: 1,
    alignSelf: "flex-start",
  },
  summaryText: {
    fontSize: 12,
    fontWeight: "700",
  },

  // Game card
  card: {
    padding: 12,
    borderRadius: 16,
    borderWidth: 1,
    gap: 10,
  },
  cardHeader: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
  },
  cardTitle: {
    fontSize: 16,
    fontWeight: "800",
  },
  cardCount: {
    fontSize: 12,
    fontWeight: "600",
  },

  // Correlation card
  corrCard: {
    borderRadius: 12,
    borderWidth: 1,
    padding: 10,
    gap: 8,
  },
  corrHeader: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "flex-start",
    gap: 10,
  },
  corrPlayers: {
    fontSize: 14,
    fontWeight: "800",
  },
  corrTeam: {
    fontSize: 11,
    fontWeight: "600",
    marginTop: 2,
  },

  // Strength badge
  strengthBadge: {
    paddingHorizontal: 8,
    paddingVertical: 5,
    borderRadius: 8,
    borderWidth: 1,
    alignItems: "center",
    minWidth: 60,
  },
  strengthLabel: {
    fontSize: 8,
    fontWeight: "800",
    letterSpacing: 0.5,
  },
  strengthValue: {
    fontSize: 14,
    fontWeight: "800",
    marginTop: 1,
  },

  // Insight
  insight: {
    fontSize: 12,
    fontWeight: "500",
    lineHeight: 17,
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

  // Markets
  marketsRow: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 6,
  },
  marketTag: {
    paddingHorizontal: 8,
    paddingVertical: 3,
    borderRadius: 6,
    borderWidth: 1,
  },
  marketTagText: {
    fontSize: 10,
    fontWeight: "700",
  },
});

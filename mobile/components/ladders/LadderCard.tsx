import { useState } from "react";
import { View, Text, StyleSheet, TouchableOpacity } from "react-native";
import { useTheme } from "@/store/useTheme";
import { CombinedOddsTable } from "./CombinedOddsTable";
import { Ladder } from "@/hooks/useLadders";
import { Ionicons } from "@expo/vector-icons";

function formatGameClock(period: string | null | undefined, clock: string | null | undefined): string | null {
  if (!period) return null;

  const periodUpper = period.toUpperCase();

  // Handle special periods
  if (periodUpper === "HALFTIME" || periodUpper === "HALF") {
    return "Halftime";
  }
  if (periodUpper === "OT" || periodUpper === "OVERTIME") {
    return clock ? `OT ${clock}` : "OT";
  }
  if (periodUpper.startsWith("OT")) {
    // OT2, OT3, etc.
    return clock ? `${period} ${clock}` : period;
  }

  // Standard quarters (Q1, Q2, Q3, Q4 or 1, 2, 3, 4)
  const quarterMatch = periodUpper.match(/^Q?(\d)$/);
  if (quarterMatch) {
    const q = `Q${quarterMatch[1]}`;
    return clock ? `${q} ${clock}` : q;
  }

  // Fallback
  return clock ? `${period} ${clock}` : period;
}

export function LadderCard({ ladder }: { ladder: Ladder }) {
  const { colors } = useTheme();
  const [analyticsExpanded, setAnalyticsExpanded] = useState(false);

  const tierColor =
    ladder.ladder_tier === "A" ? "#22c55e" :
    ladder.ladder_tier === "B" ? "#eab308" :
    colors.text.muted;

  const isLive = ladder.game_state === "LIVE";
  const hasCurrentStat = isLive && ladder.current_stat != null;
  const gameClockText = isLive && ladder.game_clock
    ? formatGameClock(ladder.game_clock.period, ladder.game_clock.clock)
    : null;

  return (
    <View style={[styles.card, { backgroundColor: colors.surface.card }]}>
      {/* Header */}
      <View style={styles.header}>
        <View style={styles.headerRow}>
          <Text style={[styles.player, { color: colors.text.primary }]}>
            {ladder.player_name}
          </Text>
          {isLive && (
            <View style={styles.liveBadge}>
              <View style={styles.liveDot} />
              <Text style={styles.liveText}>LIVE</Text>
            </View>
          )}
        </View>
        <Text style={[styles.meta, { color: colors.text.muted }]}>
          {ladder.player_team_abbr} vs {ladder.opponent_team_abbr} •{" "}
          {ladder.market.toUpperCase()}
        </Text>
        {/* Game Score and Clock for Live */}
        {isLive && (ladder.game_score || gameClockText) && (
          <View style={styles.gameInfoRow}>
            {ladder.game_score && (
              <Text style={[styles.gameScore, { color: colors.text.secondary }]}>
                {ladder.game_score.home ?? 0} - {ladder.game_score.away ?? 0}
              </Text>
            )}
            {gameClockText && (
              <Text style={[styles.gameClock, { color: "#f59e0b" }]}>
                {gameClockText}
              </Text>
            )}
          </View>
        )}
      </View>

      {/* Summary */}
      <View style={styles.summary}>
        <View style={styles.summaryItem}>
          <Text style={[styles.summaryLabel, { color: colors.text.muted }]}>Tier</Text>
          <Text style={[styles.summaryValue, { color: tierColor }]}>
            {ladder.ladder_tier}
          </Text>
        </View>
        {hasCurrentStat && (
          <View style={styles.summaryItem}>
            <Text style={[styles.summaryLabel, { color: colors.text.muted }]}>Current</Text>
            <Text style={[styles.summaryValue, { color: "#3b82f6" }]}>
              {ladder.current_stat}
            </Text>
          </View>
        )}
        <View style={styles.summaryItem}>
          <Text style={[styles.summaryLabel, { color: colors.text.muted }]}>Anchor</Text>
          <Text style={[styles.summaryValue, { color: colors.text.primary }]}>
            {ladder.anchor_line}
          </Text>
        </View>
        <View style={styles.summaryItem}>
          <Text style={[styles.summaryLabel, { color: colors.text.muted }]}>Edge</Text>
          <Text style={[styles.summaryValue, { color: "#22c55e" }]}>
            +{ladder.ladder_score.toFixed(1)}
          </Text>
        </View>
      </View>

      {/* Divider */}
      <View style={[styles.divider, { backgroundColor: colors.border.subtle }]} />

      {/* Combined Odds Table */}
      <CombinedOddsTable vendors={ladder.ladder_by_vendor} />

      {/* Expandable Analytics Section */}
      <TouchableOpacity
        style={[styles.analyticsToggle, { borderTopColor: colors.border.subtle }]}
        onPress={() => setAnalyticsExpanded(!analyticsExpanded)}
        activeOpacity={0.7}
      >
        <Text style={[styles.analyticsToggleText, { color: colors.text.secondary }]}>
          Analytics
        </Text>
        <Ionicons
          name={analyticsExpanded ? "chevron-up" : "chevron-down"}
          size={16}
          color={colors.text.secondary}
        />
      </TouchableOpacity>

      {analyticsExpanded && (
        <View style={[styles.analyticsContent, { backgroundColor: colors.surface.elevated }]}>
          <Text style={[styles.analyticsPlaceholder, { color: colors.text.muted }]}>
            Analytics data coming soon
          </Text>
        </View>
      )}
    </View>
  );
}

const styles = StyleSheet.create({
  card: {
    borderRadius: 16,
    padding: 14,
    marginBottom: 12,
  },
  header: {
    marginBottom: 8,
  },
  headerRow: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
  },
  player: {
    fontSize: 17,
    fontWeight: "700",
    flex: 1,
  },
  liveBadge: {
    flexDirection: "row",
    alignItems: "center",
    backgroundColor: "rgba(34, 197, 94, 0.15)",
    paddingHorizontal: 8,
    paddingVertical: 3,
    borderRadius: 6,
  },
  liveDot: {
    width: 6,
    height: 6,
    borderRadius: 3,
    backgroundColor: "#22c55e",
    marginRight: 4,
  },
  liveText: {
    fontSize: 10,
    fontWeight: "700",
    color: "#22c55e",
    letterSpacing: 0.5,
  },
  meta: {
    fontSize: 13,
    marginTop: 2,
  },
  gameInfoRow: {
    flexDirection: "row",
    alignItems: "center",
    marginTop: 4,
    gap: 12,
  },
  gameScore: {
    fontSize: 13,
    fontWeight: "700",
  },
  gameClock: {
    fontSize: 12,
    fontWeight: "600",
  },
  summary: {
    flexDirection: "row",
    justifyContent: "space-between",
    marginVertical: 8,
  },
  summaryItem: {
    alignItems: "center",
  },
  summaryLabel: {
    fontSize: 11,
    fontWeight: "500",
    textTransform: "uppercase",
  },
  summaryValue: {
    fontSize: 16,
    fontWeight: "700",
    marginTop: 2,
  },
  divider: {
    height: 1,
    marginVertical: 10,
  },
  analyticsToggle: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    paddingTop: 12,
    marginTop: 8,
    borderTopWidth: 1,
  },
  analyticsToggleText: {
    fontSize: 13,
    fontWeight: "600",
  },
  analyticsContent: {
    marginTop: 10,
    padding: 12,
    borderRadius: 8,
  },
  analyticsPlaceholder: {
    fontSize: 13,
    textAlign: "center",
    fontStyle: "italic",
  },
});

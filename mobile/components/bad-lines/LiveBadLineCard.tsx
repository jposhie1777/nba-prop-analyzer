// components/bad-lines/LiveBadLineCard.tsx
import {
  View,
  Text,
  StyleSheet,
  Pressable,
} from "react-native";
import { useMemo } from "react";

import { useTheme } from "@/store/useTheme";

/* ======================================================
   TYPES
====================================================== */
export type LiveBadLine = {
  game_id: number;
  player_id: number;
  player_name: string;

  home_team_abbr: string;
  away_team_abbr: string;

  market: string;
  market_window: string;
  odds_side: string;

  line_value: number;
  odds: number;

  current_stat: number;
  expected_stat: number;    // on-pace projection
  expected_edge: number;    // edge vs line (fraction)
  remaining_to_line: number;

  live_minutes: number;
  period: number;
  clock: string;
  game_state: string;

  bad_line_score: number;
};

type Props = {
  line: LiveBadLine;
};

/* ======================================================
   COMPONENT
====================================================== */
export default function LiveBadLineCard({ line }: Props) {
  const colors = useTheme((s) => s.colors);
  const styles = useMemo(() => makeStyles(colors), [colors]);

  // Severity based on edge percentage
  const edgePct = (line.expected_edge ?? 0) * 100;
  const severity =
    edgePct >= 40
      ? "high"
      : edgePct >= 25
      ? "medium"
      : "low";

  const periodLabel = line.period
    ? line.period <= 4
      ? `Q${line.period}`
      : `OT${line.period - 4}`
    : "";

  return (
    <View style={[styles.card, styles[severity]]}>
      {/* HEADER */}
      <View style={styles.header}>
        <View style={styles.headerText}>
          <Text style={styles.player} numberOfLines={1}>
            {line.player_name}
          </Text>
          <Text style={styles.matchup}>
            {line.away_team_abbr} @ {line.home_team_abbr}
          </Text>
        </View>

        {/* LIVE GAME CONTEXT */}
        <View style={styles.liveContext}>
          <View style={styles.liveDot} />
          <Text style={styles.liveText}>
            {periodLabel} {line.clock}
          </Text>
        </View>
      </View>

      {/* LINE */}
      <View style={styles.lineRow}>
        <Text style={styles.market}>
          {line.market.toUpperCase()}
        </Text>
        <Text style={styles.line}>
          {line.line_value}{" "}
          <Text style={styles.odds}>
            ({line.odds > 0 ? `+${line.odds}` : line.odds})
          </Text>
        </Text>
      </View>

      {/* LIVE STATS */}
      <View style={styles.statsRow}>
        <View style={styles.statBox}>
          <Text style={styles.statLabel}>Current</Text>
          <Text style={styles.statValue}>
            {line.current_stat?.toFixed(0) ?? "-"}
          </Text>
        </View>

        <View style={styles.statBox}>
          <Text style={styles.statLabel}>On Pace</Text>
          <Text style={[styles.statValue, styles.paceValue]}>
            {line.expected_stat?.toFixed(1) ?? "-"}
          </Text>
        </View>

        <View style={styles.statBox}>
          <Text style={styles.statLabel}>Needs</Text>
          <Text style={styles.statValue}>
            {line.remaining_to_line?.toFixed(0) ?? "-"}
          </Text>
        </View>

        <View style={styles.statBox}>
          <Text style={styles.statLabel}>Mins</Text>
          <Text style={styles.statValue}>
            {line.live_minutes?.toFixed(0) ?? "-"}
          </Text>
        </View>
      </View>

      {/* EDGE BADGE */}
      <View style={styles.edgeContainer}>
        <View style={[styles.edgeBadge, styles[`${severity}Badge`]]}>
          <Text style={styles.edgeText}>
            +{edgePct.toFixed(0)}% Edge
          </Text>
        </View>
      </View>

      {/* ACTIONS */}
      <View style={styles.actions}>
        <Pressable style={styles.actionBtn}>
          <Text style={styles.actionText}>Add</Text>
        </Pressable>
        <Pressable style={styles.actionBtn}>
          <Text style={styles.actionText}>Save</Text>
        </Pressable>
      </View>
    </View>
  );
}

/* ======================================================
   STYLES
====================================================== */
function makeStyles(colors: any) {
  return StyleSheet.create({
    card: {
      backgroundColor: colors.surface.card,
      borderRadius: 14,
      padding: 12,
      marginHorizontal: 12,
      marginVertical: 6,
      borderWidth: 1,
      borderColor: colors.border.subtle,
    },

    high: {
      borderColor: colors.accent.danger,
    },
    medium: {
      borderColor: colors.accent.warning,
    },
    low: {
      borderColor: colors.border.subtle,
    },

    header: {
      flexDirection: "row",
      alignItems: "center",
      justifyContent: "space-between",
      marginBottom: 8,
    },

    headerText: {
      flex: 1,
    },

    player: {
      fontSize: 15,
      fontWeight: "600",
      color: colors.text.primary,
    },

    matchup: {
      fontSize: 12,
      color: colors.text.muted,
      marginTop: 2,
    },

    /* Live context badge */
    liveContext: {
      flexDirection: "row",
      alignItems: "center",
      backgroundColor: colors.surface.cardSoft,
      paddingHorizontal: 8,
      paddingVertical: 4,
      borderRadius: 8,
      gap: 6,
    },

    liveDot: {
      width: 6,
      height: 6,
      borderRadius: 3,
      backgroundColor: colors.accent.success,
    },

    liveText: {
      fontSize: 11,
      fontWeight: "600",
      color: colors.text.primary,
    },

    lineRow: {
      flexDirection: "row",
      justifyContent: "space-between",
      marginBottom: 10,
    },

    market: {
      fontSize: 13,
      fontWeight: "600",
      color: colors.text.primary,
    },

    line: {
      fontSize: 13,
      color: colors.text.primary,
    },

    odds: {
      color: colors.text.muted,
    },

    /* Stats row */
    statsRow: {
      flexDirection: "row",
      justifyContent: "space-between",
      marginBottom: 10,
    },

    statBox: {
      alignItems: "center",
      flex: 1,
    },

    statLabel: {
      fontSize: 10,
      color: colors.text.muted,
      textTransform: "uppercase",
      marginBottom: 2,
    },

    statValue: {
      fontSize: 14,
      fontWeight: "600",
      color: colors.text.primary,
    },

    paceValue: {
      color: colors.accent.success,
    },

    /* Edge badge */
    edgeContainer: {
      alignItems: "flex-start",
      marginBottom: 10,
    },

    edgeBadge: {
      paddingHorizontal: 10,
      paddingVertical: 4,
      borderRadius: 12,
    },

    highBadge: {
      backgroundColor: colors.accent.danger + "30",
    },
    mediumBadge: {
      backgroundColor: colors.accent.warning + "30",
    },
    lowBadge: {
      backgroundColor: colors.accent.success + "30",
    },

    edgeText: {
      fontSize: 12,
      fontWeight: "700",
      color: colors.accent.success,
    },

    actions: {
      flexDirection: "row",
      justifyContent: "flex-end",
      gap: 12,
    },

    actionBtn: {
      paddingVertical: 6,
      paddingHorizontal: 12,
      borderRadius: 10,
      backgroundColor: colors.surface.cardSoft,
    },

    actionText: {
      fontSize: 12,
      fontWeight: "600",
      color: colors.text.primary,
    },
  });
}

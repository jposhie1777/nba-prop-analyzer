// components/bad-lines/BadLineCard.tsx
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
export type BadLine = {
  prop_id: number;
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

  hit_rate_l5?: number;
  hit_rate_l10?: number;
  hit_rate_l20?: number;

  baseline_l10?: number;
  expected_stat?: number;     // ← model projection
  expected_edge?: number;     // ← edge vs line (fraction)

  opp_allowed_rank?: number;
  defense_multiplier?: number;

  bad_line_score: number;
};


type Props = {
  line: BadLine;
  compact?: boolean;
  showMatchup?: boolean;
};

/* ======================================================
  COMPONENT
====================================================== */
export default function BadLineCard({
  line,
  compact = false,
  showMatchup = true,
}: Props) {
  // 🔑 Zustand theme (same pattern as PropCard, TrackedParlayCard, etc.)
  const colors = useTheme((s) => s.colors);
  const styles = useMemo(() => makeStyles(colors), [colors]);

  const severity =
    line.bad_line_score >= 2.5
      ? "high"
      : line.bad_line_score >= 1.75
      ? "medium"
      : "low";

  return (
    <View style={[styles.card, compact && styles.cardCompact, styles[severity]]}>
      <View style={styles.headerRow}>
        <View style={styles.playerBlock}>
          <Text style={styles.player} numberOfLines={1}>
            {line.player_name}
          </Text>
          {showMatchup && (
            <Text style={styles.matchup}>
              {line.away_team_abbr} @ {line.home_team_abbr}
            </Text>
          )}
        </View>

        <View style={styles.lineBlock}>
          <Text style={styles.market}>{line.market.toUpperCase()}</Text>
          <Text style={styles.line}>
            {line.line_value}{" "}
            <Text style={styles.odds}>
              ({line.odds > 0 ? `+${line.odds}` : line.odds})
            </Text>
          </Text>
        </View>
      </View>

      <View style={styles.metaRow}>
        {line.expected_stat != null && (
          <Text style={styles.metaText}>
            Model {line.expected_stat.toFixed(1)}
          </Text>
        )}

        {line.expected_edge != null && (
          <Text style={styles.metaText}>
            Edge +{(line.expected_edge * 100).toFixed(1)}%
          </Text>
        )}

        {line.hit_rate_l10 != null && (
          <Text style={styles.metaText}>
            L10 {(line.hit_rate_l10 * 100).toFixed(0)}%
          </Text>
        )}

        <Text style={styles.score}>
          Score {line.bad_line_score.toFixed(2)}
        </Text>
      </View>

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
      borderRadius: 12,
      padding: 10,
      marginHorizontal: 12,
      marginVertical: 6,
      borderWidth: 1,
      borderColor: colors.border.subtle,
    },
    cardCompact: {
      paddingVertical: 8,
      paddingHorizontal: 10,
      marginHorizontal: 0,
      marginVertical: 0,
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

    headerRow: {
      flexDirection: "row",
      alignItems: "flex-start",
      justifyContent: "space-between",
      gap: 10,
      marginBottom: 6,
    },

    playerBlock: {
      flex: 1,
      minWidth: 0,
    },

    player: {
      fontSize: 14,
      fontWeight: "700",
      color: colors.text.primary,
    },

    matchup: {
      fontSize: 11,
      color: colors.text.muted,
      marginTop: 2,
    },

    lineBlock: {
      alignItems: "flex-end",
    },

    market: {
      fontSize: 11,
      fontWeight: "700",
      color: colors.text.muted,
      textTransform: "uppercase",
    },

    line: {
      fontSize: 13,
      fontWeight: "600",
      color: colors.text.primary,
      marginTop: 2,
    },

    odds: {
      color: colors.text.muted,
    },

    metaRow: {
      flexDirection: "row",
      flexWrap: "wrap",
      alignItems: "center",
      gap: 8,
      marginBottom: 8,
    },

    metaText: {
      fontSize: 11,
      color: colors.text.muted,
    },

    score: {
      fontSize: 11,
      fontWeight: "700",
      color: colors.accent.warning,
    },

    actions: {
      flexDirection: "row",
      justifyContent: "flex-end",
      gap: 8,
    },

    actionBtn: {
      paddingVertical: 4,
      paddingHorizontal: 10,
      borderRadius: 8,
      backgroundColor: colors.surface.cardSoft,
    },

    actionText: {
      fontSize: 11,
      fontWeight: "600",
      color: colors.text.primary,
    },
  });
}

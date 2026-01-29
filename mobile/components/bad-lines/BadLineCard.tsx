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
};

/* ======================================================
   COMPONENT
====================================================== */
export default function BadLineCard({ line }: Props) {
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

      {/* EXPLANATION */}
      <View style={styles.explain}>
        {line.expected_stat != null && (
          <Text style={styles.explainText}>
            Model: {line.expected_stat.toFixed(1)}
          </Text>
        )}

        {line.expected_edge != null && (
          <Text style={styles.explainText}>
            Edge: +{(line.expected_edge * 100).toFixed(1)}%
          </Text>
        )}

        {line.hit_rate_l10 != null && (
          <Text style={styles.explainText}>
            L10 hit: {(line.hit_rate_l10 * 100).toFixed(0)}%
          </Text>
        )}


        <Text style={styles.score}>
          Score: {line.bad_line_score.toFixed(2)}
        </Text>
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

    lineRow: {
      flexDirection: "row",
      justifyContent: "space-between",
      marginBottom: 8,
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

    explain: {
      marginBottom: 10,
    },

    explainText: {
      fontSize: 12,
      color: colors.text.muted,
    },

    score: {
      marginTop: 4,
      fontSize: 12,
      fontWeight: "600",
      color: colors.accent.warning,
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

import {
  View,
  Text,
  StyleSheet,
  Pressable,
  Image,
} from "react-native";
import { useMemo } from "react";

import { useTheme } from "@/store/useTheme";

/* ======================================================
   TYPES
====================================================== */
export type BadLine = {
  player_id: number;
  player_name: string;
  player_image_url?: string;

  home_team_abbr: string;
  away_team_abbr: string;

  market: string;
  line_value: number;
  odds: number;

  bad_line_score: number;
  implied_edge_pct?: number;
  model_projection?: number;
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
        {line.player_image_url && (
          <Image
            source={{ uri: line.player_image_url }}
            style={styles.avatar}
          />
        )}

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
        {line.model_projection != null && (
          <Text style={styles.explainText}>
            Model: {line.model_projection.toFixed(1)}
          </Text>
        )}
        {line.implied_edge_pct != null && (
          <Text style={styles.explainText}>
            Edge: +{line.implied_edge_pct.toFixed(1)}%
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
      backgroundColor: colors.card,
      borderRadius: 14,
      padding: 12,
      marginHorizontal: 12,
      marginVertical: 6,
      borderWidth: 1,
    },

    high: {
      borderColor: colors.accent.danger,
    },
    medium: {
      borderColor: colors.accent.warning,
    },
    low: {
      borderColor: colors.border,
    },

    header: {
      flexDirection: "row",
      alignItems: "center",
      marginBottom: 8,
    },

    avatar: {
      width: 36,
      height: 36,
      borderRadius: 18,
      marginRight: 10,
      backgroundColor: colors.muted,
    },

    headerText: {
      flex: 1,
    },

    player: {
      fontSize: 15,
      fontWeight: "600",
      color: colors.text,
    },

    matchup: {
      fontSize: 12,
      color: colors.subtleText,
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
      color: colors.text,
    },

    line: {
      fontSize: 13,
      color: colors.text,
    },

    odds: {
      color: colors.subtleText,
    },

    explain: {
      marginBottom: 10,
    },

    explainText: {
      fontSize: 12,
      color: colors.subtleText,
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
      backgroundColor: colors.surface,
    },

    actionText: {
      fontSize: 12,
      fontWeight: "600",
      color: colors.text,
    },
  });
}

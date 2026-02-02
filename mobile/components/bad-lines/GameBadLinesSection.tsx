import { View, Text, StyleSheet, Pressable } from "react-native";
import { useState } from "react";
import { Ionicons } from "@expo/vector-icons";

import { useTheme } from "@/store/useTheme";
import BadLineCard, { BadLine } from "@/components/bad-lines/BadLineCard";

type Props = {
  gameId?: number;
  awayTeamAbbr: string;
  homeTeamAbbr: string;
  lines: BadLine[];
  defaultExpanded?: boolean;
};

export function GameBadLinesSection({
  awayTeamAbbr,
  homeTeamAbbr,
  lines,
  defaultExpanded = false,
}: Props) {
  const { colors } = useTheme();
  const [expanded, setExpanded] = useState(defaultExpanded);

  const awayLabel = awayTeamAbbr?.trim() || "TBD";
  const homeLabel = homeTeamAbbr?.trim() || "TBD";
  const matchup = `${awayLabel} @ ${homeLabel}`;
  const totalCount = lines.length;
  const highCount = lines.filter((line) => line.bad_line_score >= 2.5).length;
  const mediumCount = lines.filter(
    (line) => line.bad_line_score >= 1.75 && line.bad_line_score < 2.5
  ).length;

  return (
    <View style={styles.container}>
      <Pressable
        onPress={() => setExpanded(!expanded)}
        style={[
          styles.header,
          {
            backgroundColor: colors.surface?.cardSoft ?? "#222",
            borderColor: colors.border?.subtle ?? "#333",
          },
        ]}
      >
        <View style={styles.gameInfo}>
          <Text style={[styles.matchup, { color: colors.text?.primary ?? "#fff" }]}>
            {matchup}
          </Text>
          <Text style={[styles.meta, { color: colors.text?.muted ?? "#888" }]}>
            {totalCount} bad line{totalCount !== 1 ? "s" : ""}
          </Text>
        </View>

        <View style={styles.badges}>
          {highCount > 0 && (
            <View
              style={[
                styles.badge,
                { backgroundColor: colors.accent?.danger ?? "#ef4444" },
              ]}
            >
              <Text style={styles.badgeText}>{highCount} High</Text>
            </View>
          )}
          {mediumCount > 0 && (
            <View
              style={[
                styles.badge,
                { backgroundColor: colors.accent?.warning ?? "#eab308" },
              ]}
            >
              <Text style={styles.badgeText}>{mediumCount} Med</Text>
            </View>
          )}
          <Ionicons
            name={expanded ? "chevron-up" : "chevron-down"}
            size={20}
            color={colors.text?.muted ?? "#888"}
          />
        </View>
      </Pressable>

      {expanded && (
        <View
          style={[
            styles.lines,
            {
              backgroundColor: colors.surface?.card ?? "#1a1a1a",
              borderColor: colors.border?.subtle ?? "#333",
            },
          ]}
        >
          {lines.map((line) => (
            <BadLineCard
              key={
                line.prop_id ??
                `${line.player_id}-${line.market}-${line.line_value}-${line.odds}`
              }
              line={line}
              compact
              showMatchup={false}
            />
          ))}
        </View>
      )}
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    marginBottom: 10,
  },
  header: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    padding: 12,
    borderRadius: 10,
    borderWidth: StyleSheet.hairlineWidth,
  },
  gameInfo: {
    flex: 1,
  },
  matchup: {
    fontSize: 16,
    fontWeight: "700",
  },
  meta: {
    fontSize: 11,
    marginTop: 2,
  },
  badges: {
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
  },
  badge: {
    paddingHorizontal: 8,
    paddingVertical: 3,
    borderRadius: 10,
  },
  badgeText: {
    fontSize: 10,
    fontWeight: "700",
    color: "#fff",
  },
  lines: {
    marginTop: 8,
    borderRadius: 10,
    borderWidth: StyleSheet.hairlineWidth,
    paddingHorizontal: 8,
    paddingVertical: 8,
    gap: 8,
  },
});

// components/wowy/WowyCard.tsx
import { View, Text, StyleSheet, Pressable } from "react-native";
import { useState } from "react";
import { useTheme } from "@/store/useTheme";
import { InjuredPlayerWowy, TeammateWowy } from "@/lib/wowy";
import { Ionicons } from "@expo/vector-icons";

type Props = {
  data: InjuredPlayerWowy;
  defaultExpanded?: boolean;
};

function formatDiff(value: number | null): string {
  if (value === null || value === undefined) return "-";
  const sign = value > 0 ? "+" : "";
  return `${sign}${value.toFixed(1)}`;
}

function getDiffColor(value: number | null, colors: any): string {
  if (value === null || value === undefined) return colors.text?.muted ?? "#888";
  if (value > 2) return "#22c55e"; // green - big positive
  if (value > 0) return "#4ade80"; // light green
  if (value < -2) return "#ef4444"; // red - big negative
  if (value < 0) return "#f87171"; // light red
  return colors.text?.muted ?? "#888";
}

function TeammateRow({ teammate, colors }: { teammate: TeammateWowy; colors: any }) {
  return (
    <View style={styles.teammateRow}>
      <Text
        style={[styles.teammateName, { color: colors.text?.primary ?? "#fff" }]}
        numberOfLines={1}
      >
        {teammate.teammate_name}
      </Text>

      <View style={styles.statColumns}>
        {/* Points */}
        <View style={styles.statCol}>
          <Text style={[styles.statValue, { color: colors.text?.secondary ?? "#aaa" }]}>
            {teammate.pts_without?.toFixed(1) ?? "-"}
          </Text>
          <Text
            style={[
              styles.diffValue,
              { color: getDiffColor(teammate.pts_diff, colors) },
            ]}
          >
            {formatDiff(teammate.pts_diff)}
          </Text>
        </View>

        {/* Rebounds */}
        <View style={styles.statCol}>
          <Text style={[styles.statValue, { color: colors.text?.secondary ?? "#aaa" }]}>
            {teammate.reb_without?.toFixed(1) ?? "-"}
          </Text>
          <Text
            style={[
              styles.diffValue,
              { color: getDiffColor(teammate.reb_diff, colors) },
            ]}
          >
            {formatDiff(teammate.reb_diff)}
          </Text>
        </View>

        {/* Assists */}
        <View style={styles.statCol}>
          <Text style={[styles.statValue, { color: colors.text?.secondary ?? "#aaa" }]}>
            {teammate.ast_without?.toFixed(1) ?? "-"}
          </Text>
          <Text
            style={[
              styles.diffValue,
              { color: getDiffColor(teammate.ast_diff, colors) },
            ]}
          >
            {formatDiff(teammate.ast_diff)}
          </Text>
        </View>

        {/* 3PM */}
        <View style={styles.statCol}>
          <Text style={[styles.statValue, { color: colors.text?.secondary ?? "#aaa" }]}>
            {teammate.fg3m_without?.toFixed(1) ?? "-"}
          </Text>
          <Text
            style={[
              styles.diffValue,
              { color: getDiffColor(teammate.fg3m_diff, colors) },
            ]}
          >
            {formatDiff(teammate.fg3m_diff)}
          </Text>
        </View>
      </View>
    </View>
  );
}

export function WowyCard({ data, defaultExpanded = false }: Props) {
  const { colors } = useTheme();
  const [expanded, setExpanded] = useState(defaultExpanded);

  const { injured_player, team_impact, teammates } = data;

  // Sort teammates by pts_diff descending (biggest beneficiaries first)
  const sortedTeammates = [...teammates].sort(
    (a, b) => (b.pts_diff ?? 0) - (a.pts_diff ?? 0)
  );

  // Top 3 beneficiaries for collapsed view
  const topBeneficiaries = sortedTeammates.slice(0, 3);

  return (
    <View
      style={[
        styles.card,
        {
          backgroundColor: colors.surface?.card ?? "#1a1a1a",
          borderColor: colors.border?.subtle ?? "#333",
        },
      ]}
    >
      {/* Header */}
      <Pressable onPress={() => setExpanded(!expanded)} style={styles.header}>
        <View style={styles.playerSection}>
          <View style={styles.playerRow}>
            <Text
              style={[styles.playerName, { color: colors.text?.primary ?? "#fff" }]}
            >
              {injured_player.player_name}
            </Text>
            <View style={[styles.statusBadge, { backgroundColor: "#ef4444" }]}>
              <Text style={styles.statusText}>{injured_player.status}</Text>
            </View>
          </View>
          <Text style={[styles.teamText, { color: colors.text?.muted ?? "#888" }]}>
            {injured_player.team}
            {injured_player.injury_type ? ` - ${injured_player.injury_type}` : ""}
          </Text>
        </View>

        <View style={styles.impactSection}>
          <Text style={[styles.impactLabel, { color: colors.text?.muted ?? "#888" }]}>
            Team PPG
          </Text>
          <Text
            style={[
              styles.impactValue,
              { color: getDiffColor(team_impact.team_ppg_diff, colors) },
            ]}
          >
            {formatDiff(team_impact.team_ppg_diff)}
          </Text>
          <Ionicons
            name={expanded ? "chevron-up" : "chevron-down"}
            size={18}
            color={colors.text?.muted ?? "#888"}
            style={{ marginLeft: 4 }}
          />
        </View>
      </Pressable>

      {/* Games info */}
      <Text style={[styles.gamesText, { color: colors.text?.muted ?? "#888" }]}>
        {team_impact.games_without} games without • {team_impact.games_with} games with
      </Text>

      {/* Collapsed: Top beneficiaries preview */}
      {!expanded && topBeneficiaries.length > 0 && (
        <View style={styles.previewSection}>
          <Text style={[styles.previewLabel, { color: colors.text?.muted ?? "#888" }]}>
            Top beneficiaries:
          </Text>
          {topBeneficiaries.map((tm) => (
            <Text
              key={tm.player_id}
              style={[styles.previewItem, { color: colors.text?.secondary ?? "#aaa" }]}
            >
              {tm.teammate_name}:{" "}
              <Text style={{ color: getDiffColor(tm.pts_diff, colors) }}>
                {formatDiff(tm.pts_diff)} pts
              </Text>
            </Text>
          ))}
        </View>
      )}

      {/* Expanded: Full teammate table */}
      {expanded && sortedTeammates.length > 0 && (
        <View style={styles.expandedSection}>
          {/* Column headers */}
          <View style={styles.tableHeader}>
            <Text
              style={[styles.headerLabel, { color: colors.text?.muted ?? "#888" }]}
            >
              Teammate
            </Text>
            <View style={styles.statColumns}>
              <Text style={[styles.headerCol, { color: colors.text?.muted ?? "#888" }]}>
                PTS
              </Text>
              <Text style={[styles.headerCol, { color: colors.text?.muted ?? "#888" }]}>
                REB
              </Text>
              <Text style={[styles.headerCol, { color: colors.text?.muted ?? "#888" }]}>
                AST
              </Text>
              <Text style={[styles.headerCol, { color: colors.text?.muted ?? "#888" }]}>
                3PM
              </Text>
            </View>
          </View>

          {/* Teammate rows */}
          {sortedTeammates.map((tm) => (
            <TeammateRow key={tm.player_id} teammate={tm} colors={colors} />
          ))}
        </View>
      )}
    </View>
  );
}

const styles = StyleSheet.create({
  card: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 14,
    padding: 14,
    marginBottom: 12,
  },
  header: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "flex-start",
  },
  playerSection: {
    flex: 1,
  },
  playerRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
  },
  playerName: {
    fontSize: 17,
    fontWeight: "700",
  },
  statusBadge: {
    paddingHorizontal: 8,
    paddingVertical: 2,
    borderRadius: 10,
  },
  statusText: {
    fontSize: 10,
    fontWeight: "600",
    color: "#fff",
  },
  teamText: {
    fontSize: 12,
    marginTop: 4,
  },
  impactSection: {
    alignItems: "flex-end",
    flexDirection: "row",
    alignSelf: "center",
  },
  impactLabel: {
    fontSize: 11,
    marginRight: 4,
  },
  impactValue: {
    fontSize: 16,
    fontWeight: "700",
  },
  gamesText: {
    fontSize: 11,
    marginTop: 6,
  },
  previewSection: {
    marginTop: 10,
    paddingTop: 10,
    borderTopWidth: StyleSheet.hairlineWidth,
    borderTopColor: "#333",
  },
  previewLabel: {
    fontSize: 11,
    marginBottom: 4,
  },
  previewItem: {
    fontSize: 13,
    marginVertical: 2,
  },
  expandedSection: {
    marginTop: 12,
    paddingTop: 10,
    borderTopWidth: StyleSheet.hairlineWidth,
    borderTopColor: "#333",
  },
  tableHeader: {
    flexDirection: "row",
    justifyContent: "space-between",
    paddingBottom: 8,
    marginBottom: 4,
  },
  headerLabel: {
    fontSize: 11,
    fontWeight: "600",
    flex: 1,
  },
  headerCol: {
    fontSize: 10,
    fontWeight: "600",
    width: 45,
    textAlign: "center",
  },
  teammateRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    paddingVertical: 6,
  },
  teammateName: {
    fontSize: 13,
    flex: 1,
  },
  statColumns: {
    flexDirection: "row",
  },
  statCol: {
    width: 45,
    alignItems: "center",
  },
  statValue: {
    fontSize: 12,
  },
  diffValue: {
    fontSize: 11,
    fontWeight: "600",
  },
});

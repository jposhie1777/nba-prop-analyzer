// components/wowy/WowyCard.tsx
import { View, Text, StyleSheet, Pressable } from "react-native";
import { useState } from "react";
import { Ionicons } from "@expo/vector-icons";

import { useTheme } from "@/store/useTheme";
import { InjuredPlayerWowy, TeammateWowy, WowyStat } from "@/lib/wowy";

type Props = {
  data: InjuredPlayerWowy;
  stat: WowyStat;
  defaultExpanded?: boolean;
};

function formatDiff(value: number | null): string {
  if (value === null || value === undefined) return "-";
  const sign = value > 0 ? "+" : "";
  return `${sign}${value.toFixed(1)}`;
}

function getDiffColor(value: number | null): string {
  if (value === null || value === undefined) return "#888";
  if (value > 2) return "#22c55e";
  if (value > 0) return "#4ade80";
  if (value < -2) return "#ef4444";
  if (value < 0) return "#f87171";
  return "#888";
}

function getStat(tm: TeammateWowy, stat: WowyStat) {
  switch (stat) {
    case "pts":
      return { value: tm.pts_without, diff: tm.pts_diff, label: "PTS" };
    case "reb":
      return { value: tm.reb_without, diff: tm.reb_diff, label: "REB" };
    case "ast":
      return { value: tm.ast_without, diff: tm.ast_diff, label: "AST" };
    case "fg3m":
      return { value: tm.fg3m_without, diff: tm.fg3m_diff, label: "3PM" };
  }
}

export function WowyCard({ data, stat, defaultExpanded = false }: Props) {
  const { colors } = useTheme();
  const [expanded, setExpanded] = useState(defaultExpanded);

  const { injured_player, team_impact, teammates } = data;

  const sortedTeammates = [...teammates].sort((a, b) => {
    const aDiff = getStat(a, stat).diff ?? 0;
    const bDiff = getStat(b, stat).diff ?? 0;
    return bDiff - aDiff;
  });

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
      <Pressable onPress={() => setExpanded(!expanded)} style={styles.header}>
        <View style={{ flex: 1 }}>
          <Text style={[styles.playerName, { color: colors.text?.primary }]}>
            {injured_player.player_name}
          </Text>
          <Text style={[styles.teamText, { color: colors.text?.muted }]}>
            {injured_player.team ?? "—"}
            {injured_player.injury_type
              ? ` • ${injured_player.injury_type}`
              : ""}
          </Text>
        </View>

        <View style={styles.impactSection}>
          <Text style={[styles.impactValue, { color: getDiffColor(team_impact.team_ppg_diff) }]}>
            {formatDiff(team_impact.team_ppg_diff)}
          </Text>
          <Ionicons
            name={expanded ? "chevron-up" : "chevron-down"}
            size={18}
            color={colors.text?.muted}
          />
        </View>
      </Pressable>

      {!expanded && (
        <View style={styles.previewSection}>
          {topBeneficiaries.map((tm) => {
            const s = getStat(tm, stat);
            return (
              <Text key={tm.player_id} style={{ color: colors.text?.secondary }}>
                {tm.teammate_name}:{" "}
                <Text style={{ color: getDiffColor(s.diff) }}>
                  {formatDiff(s.diff)} {s.label}
                </Text>
              </Text>
            );
          })}
        </View>
      )}

      {expanded && (
        <View style={styles.expandedSection}>
          {sortedTeammates.map((tm) => {
            const s = getStat(tm, stat);
            return (
              <View key={tm.player_id} style={styles.row}>
                <Text style={[styles.name, { color: colors.text?.primary }]}>
                  {tm.teammate_name}
                </Text>
                <Text style={{ color: colors.text?.secondary }}>
                  {s.value?.toFixed(1) ?? "-"}
                </Text>
                <Text style={{ color: getDiffColor(s.diff) }}>
                  {formatDiff(s.diff)}
                </Text>
              </View>
            );
          })}
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
  header: { flexDirection: "row", justifyContent: "space-between" },
  playerName: { fontSize: 16, fontWeight: "700" },
  teamText: { fontSize: 12, marginTop: 4 },
  impactSection: { flexDirection: "row", alignItems: "center" },
  impactValue: { fontSize: 16, fontWeight: "700", marginRight: 6 },

  previewSection: { marginTop: 10 },
  expandedSection: { marginTop: 10 },

  row: {
    flexDirection: "row",
    justifyContent: "space-between",
    paddingVertical: 4,
  },
  name: { flex: 1 },
});

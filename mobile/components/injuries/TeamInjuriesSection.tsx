// components/injuries/TeamInjuriesSection.tsx
import { View, Text, StyleSheet, Pressable } from "react-native";
import { useState } from "react";
import { useTheme } from "@/store/useTheme";
import { InjuryRecord, TeamInjuries } from "@/lib/injuries";
import { Ionicons } from "@expo/vector-icons";

type Props = {
  teamData: TeamInjuries;
  defaultExpanded?: boolean;
};

function getStatusColor(status: string, colors: any): string {
  switch (status.toLowerCase()) {
    case "out":
      return colors.accent?.danger ?? "#ef4444";
    case "doubtful":
      return "#f97316";
    case "questionable":
      return "#eab308";
    case "day-to-day":
      return "#3b82f6";
    case "probable":
      return "#22c55e";
    default:
      return colors.text?.muted ?? "#888";
  }
}

function getInjuryMeta(injury: InjuryRecord): string | null {
  const parts: string[] = [];
  if (injury.injury_type) {
    parts.push(injury.injury_type);
  }
  if (injury.report_date) {
    parts.push(`Reported ${injury.report_date}`);
  }
  return parts.length > 0 ? parts.join(" | ") : null;
}

export function TeamInjuriesSection({ teamData, defaultExpanded = true }: Props) {
  const { colors } = useTheme();
  const [expanded, setExpanded] = useState(defaultExpanded);

  const outCount = teamData.injuries.filter((i) => i.status === "Out").length;
  const questionableCount = teamData.injuries.filter(
    (i) => i.status === "Questionable" || i.status === "Doubtful"
  ).length;
  const totalCount = teamData.injuries.length;

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
        <View style={styles.teamInfo}>
          <Text
            style={[styles.teamAbbr, { color: colors.text?.primary ?? "#fff" }]}
          >
            {teamData.team}
          </Text>
          {teamData.team_name && (
            <Text
              style={[styles.teamName, { color: colors.text?.muted ?? "#888" }]}
            >
              {teamData.team_name}
            </Text>
          )}
          <Text style={[styles.teamMeta, { color: colors.text?.muted ?? "#888" }]}>
            {totalCount} {totalCount === 1 ? "injury" : "injuries"}
          </Text>
        </View>

        <View style={styles.badges}>
          {outCount > 0 && (
            <View style={[styles.badge, { backgroundColor: "#ef4444" }]}>
              <Text style={styles.badgeText}>{outCount} OUT</Text>
            </View>
          )}
          {questionableCount > 0 && (
            <View style={[styles.badge, { backgroundColor: "#eab308" }]}>
              <Text style={styles.badgeText}>{questionableCount} Q</Text>
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
            styles.injuries,
            {
              backgroundColor: colors.surface?.card ?? "#1a1a1a",
              borderColor: colors.border?.subtle ?? "#333",
            },
          ]}
        >
          {teamData.injuries.map((injury, index) => {
            const statusColor = getStatusColor(injury.status, colors);
            const metaText = getInjuryMeta(injury);
            return (
              <View
                key={injury.injury_id ?? injury.player_id}
                style={[
                  styles.injuryRow,
                  index > 0 && {
                    borderTopWidth: StyleSheet.hairlineWidth,
                    borderColor: colors.border?.subtle ?? "#333",
                  },
                ]}
              >
                <View style={styles.injuryInfo}>
                  <Text
                    style={[
                      styles.playerName,
                      { color: colors.text?.primary ?? "#fff" },
                    ]}
                  >
                    {injury.player_name}
                  </Text>
                  {metaText && (
                    <Text
                      style={[
                        styles.injuryMeta,
                        { color: colors.text?.muted ?? "#888" },
                      ]}
                    >
                      {metaText}
                    </Text>
                  )}
                </View>
                <View style={styles.injuryRight}>
                  <View style={[styles.statusBadge, { backgroundColor: statusColor }]}>
                    <Text style={styles.statusText}>{injury.status}</Text>
                  </View>
                  {injury.return_date && (
                    <Text
                      style={[
                        styles.returnText,
                        { color: colors.text?.muted ?? "#888" },
                      ]}
                    >
                      ETA {injury.return_date}
                    </Text>
                  )}
                </View>
              </View>
            );
          })}
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
  teamInfo: {
    flex: 1,
  },
  teamAbbr: {
    fontSize: 18,
    fontWeight: "700",
  },
  teamName: {
    fontSize: 12,
    marginTop: 2,
  },
  teamMeta: {
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
  injuries: {
    marginTop: 8,
    borderRadius: 10,
    borderWidth: StyleSheet.hairlineWidth,
    overflow: "hidden",
  },
  injuryRow: {
    paddingVertical: 10,
    paddingHorizontal: 12,
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "flex-start",
    gap: 10,
  },
  injuryInfo: {
    flex: 1,
  },
  playerName: {
    fontSize: 14,
    fontWeight: "700",
  },
  injuryMeta: {
    fontSize: 11,
    marginTop: 2,
  },
  injuryRight: {
    alignItems: "flex-end",
    gap: 4,
  },
  statusBadge: {
    paddingHorizontal: 8,
    paddingVertical: 3,
    borderRadius: 10,
  },
  statusText: {
    fontSize: 10,
    fontWeight: "700",
    color: "#fff",
  },
  returnText: {
    fontSize: 10,
  },
});

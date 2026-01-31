// components/injuries/InjuryCard.tsx
import { View, Text, StyleSheet } from "react-native";
import { useTheme } from "@/store/useTheme";
import { InjuryRecord } from "@/lib/injuries";

type Props = {
  injury: InjuryRecord;
};

function getStatusColor(status: string, colors: any): string {
  switch (status.toLowerCase()) {
    case "out":
      return colors.accent?.danger ?? "#ef4444";
    case "doubtful":
      return "#f97316"; // orange
    case "questionable":
      return "#eab308"; // yellow
    case "day-to-day":
      return "#3b82f6"; // blue
    case "probable":
      return "#22c55e"; // green
    default:
      return colors.text?.muted ?? "#888";
  }
}

export function InjuryCard({ injury }: Props) {
  const { colors } = useTheme();
  const statusColor = getStatusColor(injury.status, colors);

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
      <View style={styles.header}>
        <View style={styles.playerInfo}>
          <Text
            style={[styles.playerName, { color: colors.text?.primary ?? "#fff" }]}
          >
            {injury.player_name}
          </Text>
          <Text
            style={[styles.teamName, { color: colors.text?.muted ?? "#888" }]}
          >
            {injury.team_abbreviation}
            {injury.team_name ? ` - ${injury.team_name}` : ""}
          </Text>
        </View>
        <View style={[styles.statusBadge, { backgroundColor: statusColor }]}>
          <Text style={styles.statusText}>{injury.status}</Text>
        </View>
      </View>

      {injury.injury_type && (
        <Text
          style={[styles.injuryType, { color: colors.text?.secondary ?? "#aaa" }]}
        >
          {injury.injury_type}
        </Text>
      )}

      <View style={styles.footer}>
        {injury.report_date && (
          <Text style={[styles.date, { color: colors.text?.muted ?? "#888" }]}>
            Reported: {injury.report_date}
          </Text>
        )}
        {injury.return_date && (
          <Text style={[styles.date, { color: colors.text?.muted ?? "#888" }]}>
            Est. Return: {injury.return_date}
          </Text>
        )}
      </View>
    </View>
  );
}

const styles = StyleSheet.create({
  card: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 12,
    padding: 14,
    marginBottom: 10,
  },
  header: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "flex-start",
  },
  playerInfo: {
    flex: 1,
  },
  playerName: {
    fontSize: 16,
    fontWeight: "700",
  },
  teamName: {
    fontSize: 12,
    marginTop: 2,
  },
  statusBadge: {
    paddingHorizontal: 10,
    paddingVertical: 4,
    borderRadius: 12,
  },
  statusText: {
    fontSize: 12,
    fontWeight: "600",
    color: "#fff",
  },
  injuryType: {
    fontSize: 14,
    marginTop: 8,
    fontWeight: "500",
  },
  footer: {
    marginTop: 8,
    flexDirection: "row",
    justifyContent: "space-between",
  },
  date: {
    fontSize: 11,
  },
});

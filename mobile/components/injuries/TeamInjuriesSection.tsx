// components/injuries/TeamInjuriesSection.tsx
import { View, Text, StyleSheet, Pressable } from "react-native";
import { useState } from "react";
import { useTheme } from "@/store/useTheme";
import { TeamInjuries } from "@/lib/injuries";
import { InjuryCard } from "./InjuryCard";
import { Ionicons } from "@expo/vector-icons";

type Props = {
  teamData: TeamInjuries;
  defaultExpanded?: boolean;
};

export function TeamInjuriesSection({ teamData, defaultExpanded = true }: Props) {
  const { colors } = useTheme();
  const [expanded, setExpanded] = useState(defaultExpanded);

  const outCount = teamData.injuries.filter((i) => i.status === "Out").length;
  const questionableCount = teamData.injuries.filter(
    (i) => i.status === "Questionable" || i.status === "Doubtful"
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
        <View style={styles.injuries}>
          {teamData.injuries.map((injury) => (
            <InjuryCard key={injury.injury_id ?? injury.player_id} injury={injury} />
          ))}
        </View>
      )}
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    marginBottom: 12,
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
    paddingLeft: 8,
  },
});

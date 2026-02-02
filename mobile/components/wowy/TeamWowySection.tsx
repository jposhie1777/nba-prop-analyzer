// components/wowy/TeamWowySection.tsx
import { View, Text, StyleSheet, Pressable } from "react-native";
import { useState } from "react";
import { Ionicons } from "@expo/vector-icons";

import { useTheme } from "@/store/useTheme";
import { InjuredPlayerWowy, WowyStat } from "@/lib/wowy";
import { WowyCard } from "@/components/wowy/WowyCard";

type Props = {
  team: string;
  players: InjuredPlayerWowy[];
  stat: WowyStat;
  defaultExpanded?: boolean;
};

export function TeamWowySection({
  team,
  players,
  stat,
  defaultExpanded = false,
}: Props) {
  const { colors } = useTheme();
  const [expanded, setExpanded] = useState(defaultExpanded);

  const outCount = players.filter(
    (p) => p.injured_player.status === "Out"
  ).length;
  const questionableCount = players.filter((p) =>
    ["Questionable", "Doubtful"].includes(p.injured_player.status)
  ).length;
  const totalCount = players.length;
  const teamLabel = team?.trim() ? team : "Unknown";

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
          <Text style={[styles.teamAbbr, { color: colors.text?.primary ?? "#fff" }]}>
            {teamLabel}
          </Text>
          <Text style={[styles.teamMeta, { color: colors.text?.muted ?? "#888" }]}>
            {totalCount} injured player{totalCount !== 1 ? "s" : ""}
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
              <Text style={styles.badgeText}>{questionableCount} GTD</Text>
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
        <View style={styles.players}>
          {players.map((player) => (
            <WowyCard
              key={player.injured_player.player_id}
              data={player}
              stat={stat}
              defaultExpanded={false}
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
  teamInfo: {
    flex: 1,
  },
  teamAbbr: {
    fontSize: 18,
    fontWeight: "700",
  },
  teamMeta: {
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
  players: {
    marginTop: 8,
    paddingLeft: 6,
  },
});

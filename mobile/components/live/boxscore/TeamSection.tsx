// components/live/boxscore/TeamSection.tsx
import { useState } from "react";
import { View, Text, Pressable, StyleSheet } from "react-native";
import { LivePlayerStat } from "@/hooks/useLivePlayerStats";
import { PlayerRow } from "./PlayerRow";
import { useTheme } from "@/store/useTheme";

export function TeamSection({
  label,
  players,
}: {
  label: string;
  players: LivePlayerStat[];
}) {
  const { colors } = useTheme();
  const [expanded, setExpanded] = useState(false);

  if (!players.length) return null;

  const sorted = [...players].sort(
    (a, b) => parseFloat(b.minutes || "0") - parseFloat(a.minutes || "0")
  );

  const starters = sorted.slice(0, 5);
  const bench = sorted.slice(5);

  return (
    <View style={styles.section}>
      <Pressable onPress={() => setExpanded(!expanded)}>
        <Text style={[styles.team, { color: colors.text.secondary }]}>
          {label} {expanded ? "▾" : "▸"}
        </Text>
      </Pressable>

      {starters.map((p) => (
        <PlayerRow key={p.player_id} player={p} active />
      ))}

      {expanded &&
        bench.map((p) => (
          <PlayerRow key={p.player_id} player={p} />
        ))}
    </View>
  );
}

const styles = StyleSheet.create({
  section: {
    marginBottom: 8,
  },
  team: {
    fontSize: 12,
    marginBottom: 4,
  },
});
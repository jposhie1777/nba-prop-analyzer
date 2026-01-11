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

  // 🔴 DEBUG: confirm section receives players
  if (!players.length) {
    return (
      <Text
        style={{
          fontSize: 10,
          textAlign: "center",
          color: colors.text.muted,
          marginBottom: 4,
        }}
      >
        DEBUG TeamSection ({label}): no players
      </Text>
    );
  }

  // 🔧 FIX: safe numeric sorting (minutes is number | null now)
  const sorted = [...players].sort(
    (a, b) => (b.minutes ?? 0) - (a.minutes ?? 0)
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
  
      {/* 🔴 DEBUG 8 (optional, can remove later) */}
      <Text
        style={{
          fontSize: 10,
          color: colors.text.muted,
          marginBottom: 2,
        }}
      >
        DEBUG first player: {players[0]?.name} ({players[0]?.team})
      </Text>
  
      {/* ✅ STAT HEADERS — ADD THIS */}
      <View style={styles.statHeaderRow}>
        <Text style={styles.nameSpacer}> </Text>
        <Text style={styles.statHeader}>PTS</Text>
        <Text style={styles.statHeader}>REB</Text>
        <Text style={styles.statHeader}>AST</Text>
        <Text style={styles.statHeader}>MIN</Text>
        <Text style={styles.statHeader}>3PT</Text>
      </View>
  
      {/* PLAYERS */}
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
  statHeaderRow: {
    flexDirection: "row",
    alignItems: "center",
    paddingHorizontal: 4,
    marginBottom: 4,
  },
  
  nameSpacer: {
    flex: 1, // aligns with player name column
  },
  
  statHeader: {
    width: 36,
    textAlign: "center",
    fontSize: 11,
    fontWeight: "600",
    color: "#9CA3AF", // or colors.text.muted
  },
});
  
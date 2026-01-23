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

  // 🔴 DEV GUARD: no players for team (terminal only)
  if (!players.length) {
    if (__DEV__) {
      console.warn("TEAMSECTION GUARD: no players", { label });
    }
    return null;
  }

  // 🔧 Safe numeric sorting (minutes is number | null)
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

      {/* STAT HEADERS */}
      <View style={styles.statHeaderRow}>
        <Text style={styles.nameSpacer}> </Text>
        <Text style={[styles.statHeader, { color: colors.text.muted }]}>
          PTS
        </Text>
        <Text style={[styles.statHeader, { color: colors.text.muted }]}>
          REB
        </Text>
        <Text style={[styles.statHeader, { color: colors.text.muted }]}>
          AST
        </Text>
        <Text style={[styles.statHeader, { color: colors.text.muted }]}>
          MIN
        </Text>
        <Text style={[styles.statHeader, { color: colors.text.muted }]}>
          3PT
        </Text>
        <Text style={[styles.pmHeader, { color: colors.text.muted }]}>
          +/-
        </Text>
      </View>

      {/* STARTERS */}
      {starters.map((p) => (
        <PlayerRow key={p.player_id} player={p} active />
      ))}

      {/* BENCH */}
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
    flex: 1,
  },
  statHeader: {
    width: 36,
    textAlign: "center",
    fontSize: 11,
    fontWeight: "600",
  },
  pmHeader: {
    width: 44,
    textAlign: "center",
    fontSize: 11,
    fontWeight: "600",
  },
});
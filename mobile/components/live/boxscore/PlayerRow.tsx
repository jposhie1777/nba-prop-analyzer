// components/live/boxscore/PlayerRow.tsx
import { useEffect, useRef } from "react";
import { View, Text, StyleSheet, Animated } from "react-native";
import { LivePlayerStat } from "@/hooks/useLivePlayerStats";
import { StatPill } from "./StatPill";
import { useTheme } from "@/store/useTheme";

export function PlayerRow({
  player,
  active = false,
}: {
  player: LivePlayerStat;
  active?: boolean;
}) {
  const { colors } = useTheme();
  const flash = useRef(new Animated.Value(0)).current;
  const prevPts = useRef(player.pts);

  useEffect(() => {
    if (player.pts !== prevPts.current) {
      Animated.sequence([
        Animated.timing(flash, {
          toValue: 1,
          duration: 120,
          useNativeDriver: false,
        }),
        Animated.timing(flash, {
          toValue: 0,
          duration: 400,
          useNativeDriver: false,
        }),
      ]).start();
      prevPts.current = player.pts;
    }
  }, [player.pts]);

  const bg = flash.interpolate({
    inputRange: [0, 1],
    outputRange: ["transparent", colors.glow.success],
  });

  return (
    <Animated.View
      style={[
        styles.row,
        { backgroundColor: bg },
        active && { borderLeftColor: colors.accent.primary },
      ]}
    >
      <Text style={styles.name}>{player.name}</Text>
  
      <Text style={styles.stat}>{player.pts}</Text>
      <Text style={styles.stat}>{player.reb}</Text>
      <Text style={styles.stat}>{player.ast}</Text>
  
      {/* 🔴 NEW */}
      <Text style={styles.stat}>
        {player.minutes ?? "—"}
      </Text>
  
      {/* 🔴 NEW */}
      <Text style={styles.stat}>
        {player.fg3?.[0] ?? 0}-{player.fg3?.[1] ?? 0}
      </Text>
  
      <StatPill value={player.plus_minus} />
    </Animated.View>
  );
}

const styles = StyleSheet.create({
  row: {
    flexDirection: "row",
    alignItems: "center",
    paddingVertical: 4,
    paddingLeft: 6,
    borderLeftWidth: 3,
    borderLeftColor: "transparent",
  },
  name: {
    flex: 1,
    fontSize: 13,
  },
  stat: {
    width: 36,        // ⬅️ change from 28
    textAlign: "center",
    fontSize: 12,
  },
});
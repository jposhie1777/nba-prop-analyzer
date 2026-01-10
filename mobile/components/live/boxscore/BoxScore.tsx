// components/live/boxscore/BoxScore.tsx
import { View, Text, StyleSheet } from "react-native";
import { useTheme } from "@/store/useTheme";
import { LivePlayerStat } from "@/hooks/useLivePlayerStats";
import { TeamSection } from "./TeamSection";

export function BoxScore({
  homeTeam,
  awayTeam,
  players,
}: {
  homeTeam: string;
  awayTeam: string;
  players: LivePlayerStat[];
}) {
  const { colors } = useTheme();

  // 🔴 DEBUG: no players attached to this game
  if (!players.length) {
    return (
      <Text
        style={{
          fontSize: 10,
          color: "red",
          textAlign: "center",
          marginVertical: 4,
        }}
      >
        DEBUG BoxScore hidden — no players
      </Text>
    );
  }

  const period = players[0]?.period;
  const clock = players[0]?.clock;

  const home = players.filter((p) => p.team === homeTeam);
  const away = players.filter((p) => p.team === awayTeam);

  // 🔴 DEBUG: console output (Expo / GitHub terminal)
  console.log("BOX DEBUG", {
    game: players[0]?.game_id,
    homeTeam,
    awayTeam,
    total: players.length,
    homeCount: home.length,
    awayCount: away.length,
  });

  return (
    <View>
      {/* 🔴 DEBUG: visible in mobile UI */}
      <Text
        style={{
          fontSize: 10,
          textAlign: "center",
          color: colors.text.muted,
          marginBottom: 4,
        }}
      >
        DEBUG BoxScore — total:{players.length} home:{home.length} away:
        {away.length}
      </Text>

      {(period || clock) && (
        <Text style={[styles.clock, { color: colors.text.muted }]}>
          Q{period} · {clock}
        </Text>
      )}

      <TeamSection label={awayTeam} players={away} />
      <TeamSection label={homeTeam} players={home} />
    </View>
  );
}

const styles = StyleSheet.create({
  clock: {
    fontSize: 12,
    textAlign: "center",
    marginBottom: 6,
  },
});
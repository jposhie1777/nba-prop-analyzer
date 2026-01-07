import { View, Text, StyleSheet } from "react-native";
import { useTheme } from "@/store/useTheme";
import { LivePlayerStat } from "@/hooks/useLivePlayerStats";

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

  const home = players.filter((p) => p.team === homeTeam);
  const away = players.filter((p) => p.team === awayTeam);

  if (!players.length) return null;

  return (
    <View>
      <TeamSection label={awayTeam} players={away} />
      <TeamSection label={homeTeam} players={home} />
    </View>
  );
}

function TeamSection({
  label,
  players,
}: {
  label: string;
  players: LivePlayerStat[];
}) {
  const { colors } = useTheme();

  if (!players.length) return null;

  return (
    <View style={styles.section}>
      <Text style={[styles.team, { color: colors.text.muted }]}>
        {label}
      </Text>

      {players.map((p) => (
        <View key={p.player_id} style={styles.row}>
          <Text style={styles.name}>{p.name}</Text>
          <Text style={styles.stat}>{p.pts} PTS</Text>
          <Text style={styles.stat}>{p.reb} REB</Text>
          <Text style={styles.stat}>{p.ast} AST</Text>
        </View>
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
  row: {
    flexDirection: "row",
    justifyContent: "space-between",
    paddingVertical: 4,
  },
  name: {
    flex: 1,
    fontSize: 13,
  },
  stat: {
    width: 50,
    textAlign: "right",
    fontSize: 12,
  },
});
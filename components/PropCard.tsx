import { View, Text, StyleSheet } from "react-native";
import colors from "../theme/colors";
import text from "../theme/text";

type PropCardProps = {
  matchup?: string;
  player?: string;
  market?: string;
  hitRate?: number;
  edge?: number;
  confidence?: number;
};

export default function PropCard({
  matchup = "CHA @ MIL",
  player = "Miles Bridges",
  market = "Points • Over 18.5",
  hitRate = 82,
  edge = 11,
  confidence = 78,
}: PropCardProps) {
  return (
    <View style={styles.card}>
      {/* Matchup */}
      <Text style={styles.matchup}>{matchup}</Text>

      {/* Player */}
      <Text style={styles.player}>{player}</Text>

      {/* Market */}
      <Text style={styles.market}>{market}</Text>

      {/* Decision row */}
      <View style={styles.row}>
        <Text style={styles.hit}>{hitRate}% HIT</Text>
        <Text style={styles.edge}>+{edge}% EDGE</Text>
      </View>

      {/* Divider */}
      <View style={styles.divider} />

      {/* Confidence */}
      <Text style={styles.confidence}>
        Confidence ▰▰▰▰▰▰▱▱ {confidence}
      </Text>
    </View>
  );
}

const styles = StyleSheet.create({
  card: {
    backgroundColor: colors.card,
    borderRadius: 20,
    padding: 16,
    marginHorizontal: 16,
    marginVertical: 12,
  },

  matchup: {
    color: colors.textSecondary,
    fontSize: text.label,
    marginBottom: 6,
  },

  player: {
    color: colors.textPrimary,
    fontSize: text.title,
    fontWeight: "600",
    marginBottom: 2,
  },

  market: {
    color: colors.textSecondary,
    fontSize: text.subtitle,
    marginBottom: 12,
  },

  row: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    marginBottom: 10,
  },

  hit: {
    color: colors.success,
    fontSize: text.stat,
    fontWeight: "600",
  },

  edge: {
    color: colors.accent,
    fontSize: text.stat,
    fontWeight: "600",
  },

  divider: {
    height: 1,
    backgroundColor: colors.divider,
    marginVertical: 8,
  },

  confidence: {
    color: colors.textSecondary,
    fontSize: text.label,
  },
});
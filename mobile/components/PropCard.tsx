import { View, Text, StyleSheet } from "react-native";
import colors from "../theme/color";
import textStyles from "../theme/text";

type PropCardProps = {
  player: string;
  market: string;

  line: number;
  odds: number;

  hitRate: number; // 0–1
  edge: number;    // 0–1

  home: string;
  away: string;

  confidence?: number;
};

export default function PropCard({
  player,
  market,
  line,
  odds,
  hitRate,
  edge,
  home,
  away,
  confidence,
}: PropCardProps) {
  const hitPct = Math.round(hitRate * 100);
  const edgePct = Math.round(edge * 100);

  return (
    <View style={styles.card}>
      {/* Matchup */}
      <Text style={styles.matchup}>
        {away} @ {home}
      </Text>

      {/* Player */}
      <Text style={styles.player}>{player}</Text>

      {/* Market */}
      <Text style={styles.market}>
        {market} • {line}
      </Text>

      {/* Decision row */}
      <View style={styles.row}>
        <Text style={styles.hit}>{hitPct}% HIT</Text>
        <Text style={styles.edge}>+{edgePct}% EDGE</Text>
      </View>

      {/* Divider */}
      <View style={styles.divider} />

      {/* Confidence */}
      {confidence !== undefined && (
        <Text style={styles.confidence}>
          Confidence ▰▰▰▰▰▰▱▱ {confidence}
        </Text>
      )}
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
    fontSize: textStyles.label,
    marginBottom: 6,
  },

  player: {
    color: colors.textPrimary,
    fontSize: textStyles.title,
    fontWeight: "600",
    marginBottom: 2,
  },

  market: {
    color: colors.textSecondary,
    fontSize: textStyles.subtitle,
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
    fontSize: textStyles.stat,
    fontWeight: "600",
  },

  edge: {
    color: colors.accent,
    fontSize: textStyles.stat,
    fontWeight: "600",
  },

  divider: {
    height: 1,
    backgroundColor: colors.divider,
    marginVertical: 8,
  },

  confidence: {
    color: colors.textSecondary,
    fontSize: textStyles.label,
  },
});
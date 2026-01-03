import { View, Text, StyleSheet } from "react-native";

export default function PropCard() {
  return (
    <View style={styles.card}>
      <Text style={styles.matchup}>CHA @ MIL</Text>

      <Text style={styles.player}>Miles Bridges</Text>
      <Text style={styles.market}>Points • Over 18.5</Text>

      <View style={styles.row}>
        <Text style={styles.stat}>82% HIT</Text>
        <Text style={styles.edge}>+11% EDGE</Text>
      </View>

      <Text style={styles.confidence}>Confidence ▰▰▰▰▰▰▱▱ 78</Text>
    </View>
  );
}

const styles = StyleSheet.create({
  card: {
    backgroundColor: "#141A2E",
    borderRadius: 20,
    padding: 16,
    margin: 16,
  },
  matchup: {
    color: "#A8B0D3",
    fontSize: 12,
    marginBottom: 6,
  },
  player: {
    color: "#FFFFFF",
    fontSize: 18,
    fontWeight: "600",
  },
  market: {
    color: "#A8B0D3",
    fontSize: 14,
    marginBottom: 12,
  },
  row: {
    flexDirection: "row",
    justifyContent: "space-between",
    marginBottom: 10,
  },
  stat: {
    color: "#3DFFB5",
    fontSize: 16,
    fontWeight: "600",
  },
  edge: {
    color: "#6C7CFF",
    fontSize: 16,
    fontWeight: "600",
  },
  confidence: {
    color: "#A8B0D3",
    fontSize: 12,
  },
});
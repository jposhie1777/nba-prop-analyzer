import { View, Text, StyleSheet } from "react-native";
import { useTheme } from "@/store/useTheme";
import { VendorLadder } from "./VendorLadder";

export function LadderCard({ ladder }: { ladder: any }) {
  const { colors } = useTheme();

  return (
    <View style={[styles.card, { backgroundColor: colors.card }]}>
      {/* Header */}
      <View style={styles.header}>
        <Text style={styles.player}>{ladder.player_name}</Text>
        <Text style={styles.meta}>
          {ladder.player_team_abbr} vs {ladder.opponent_team_abbr} •{" "}
          {ladder.market.toUpperCase()}
        </Text>
      </View>

      {/* Summary */}
      <View style={styles.summary}>
        <Text>Tier: {ladder.ladder_tier}</Text>
        <Text>Anchor: {ladder.anchor_line}</Text>
        <Text>Edge: +{ladder.ladder_score.toFixed(1)}</Text>
      </View>

      {/* Vendors */}
      {ladder.ladder_by_vendor.map((v: any) => (
        <VendorLadder key={v.vendor} vendorBlock={v} />
      ))}
    </View>
  );
}

const styles = StyleSheet.create({
  card: {
    borderRadius: 12,
    padding: 12,
  },
  header: {
    marginBottom: 6,
  },
  player: {
    fontSize: 16,
    fontWeight: "700",
  },
  meta: {
    fontSize: 12,
    opacity: 0.7,
  },
  summary: {
    flexDirection: "row",
    justifyContent: "space-between",
    marginVertical: 8,
  },
});
import { View, Text, StyleSheet } from "react-native";
import { useTheme } from "@/store/useTheme";
import { VendorLadder } from "./VendorLadder";
import { Ladder } from "@/hooks/useLadders";

export function LadderCard({ ladder }: { ladder: Ladder }) {
  const { colors } = useTheme();

  const tierColor =
    ladder.ladder_tier === "A" ? "#22c55e" :
    ladder.ladder_tier === "B" ? "#eab308" :
    colors.text.muted;

  return (
    <View style={[styles.card, { backgroundColor: colors.surface.card }]}>
      {/* Header */}
      <View style={styles.header}>
        <Text style={[styles.player, { color: colors.text.primary }]}>
          {ladder.player_name}
        </Text>
        <Text style={[styles.meta, { color: colors.text.muted }]}>
          {ladder.player_team_abbr} vs {ladder.opponent_team_abbr} •{" "}
          {ladder.market.toUpperCase()}
        </Text>
      </View>

      {/* Summary */}
      <View style={styles.summary}>
        <View style={styles.summaryItem}>
          <Text style={[styles.summaryLabel, { color: colors.text.muted }]}>Tier</Text>
          <Text style={[styles.summaryValue, { color: tierColor }]}>
            {ladder.ladder_tier}
          </Text>
        </View>
        <View style={styles.summaryItem}>
          <Text style={[styles.summaryLabel, { color: colors.text.muted }]}>Anchor</Text>
          <Text style={[styles.summaryValue, { color: colors.text.primary }]}>
            {ladder.anchor_line}
          </Text>
        </View>
        <View style={styles.summaryItem}>
          <Text style={[styles.summaryLabel, { color: colors.text.muted }]}>Edge</Text>
          <Text style={[styles.summaryValue, { color: "#22c55e" }]}>
            +{ladder.ladder_score.toFixed(1)}
          </Text>
        </View>
      </View>

      {/* Divider */}
      <View style={[styles.divider, { backgroundColor: colors.border.subtle }]} />

      {/* Vendors */}
      {ladder.ladder_by_vendor.map((v) => (
        <VendorLadder key={v.vendor} vendorBlock={v} />
      ))}
    </View>
  );
}

const styles = StyleSheet.create({
  card: {
    borderRadius: 16,
    padding: 14,
    marginBottom: 12,
  },
  header: {
    marginBottom: 8,
  },
  player: {
    fontSize: 17,
    fontWeight: "700",
  },
  meta: {
    fontSize: 13,
    marginTop: 2,
  },
  summary: {
    flexDirection: "row",
    justifyContent: "space-between",
    marginVertical: 8,
  },
  summaryItem: {
    alignItems: "center",
  },
  summaryLabel: {
    fontSize: 11,
    fontWeight: "500",
    textTransform: "uppercase",
  },
  summaryValue: {
    fontSize: 16,
    fontWeight: "700",
    marginTop: 2,
  },
  divider: {
    height: 1,
    marginVertical: 10,
  },
});
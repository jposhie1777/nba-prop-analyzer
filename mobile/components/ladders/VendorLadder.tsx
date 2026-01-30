import { View, Text, StyleSheet } from "react-native";
import { useTheme } from "@/store/useTheme";
import { VendorBlock } from "@/hooks/useLadders";

export function VendorLadder({ vendorBlock }: { vendorBlock: VendorBlock }) {
  const { colors } = useTheme();

  return (
    <View style={styles.container}>
      <Text style={[styles.vendor, { color: colors.text.secondary }]}>
        {vendorBlock.vendor.toUpperCase()}
      </Text>

      <View style={[styles.header, { borderBottomColor: colors.border.subtle }]}>
        <Text style={[styles.headerText, { color: colors.text.muted }]}>Line</Text>
        <Text style={[styles.headerText, styles.oddsCol, { color: colors.text.muted }]}>Odds</Text>
        <Text style={[styles.headerText, styles.scoreCol, { color: colors.text.muted }]}>Edge</Text>
      </View>

      {vendorBlock.rungs.map((r, i) => (
        <View key={i} style={styles.rung}>
          <Text style={[styles.line, { color: colors.text.primary }]}>
            {r.line}
          </Text>
          <Text style={[styles.odds, { color: r.odds < 0 ? "#22c55e" : colors.text.primary }]}>
            {r.odds > 0 ? `+${r.odds}` : r.odds}
          </Text>
          <Text style={[styles.score, { color: "#22c55e" }]}>
            +{r.ladder_score.toFixed(1)}
          </Text>
        </View>
      ))}
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    marginTop: 10,
  },
  vendor: {
    fontSize: 12,
    fontWeight: "700",
    marginBottom: 6,
    letterSpacing: 0.5,
  },
  header: {
    flexDirection: "row",
    justifyContent: "space-between",
    paddingBottom: 4,
    marginBottom: 4,
    borderBottomWidth: 1,
  },
  headerText: {
    fontSize: 10,
    fontWeight: "600",
    textTransform: "uppercase",
  },
  oddsCol: {
    width: 60,
    textAlign: "right",
  },
  scoreCol: {
    width: 50,
    textAlign: "right",
  },
  rung: {
    flexDirection: "row",
    justifyContent: "space-between",
    paddingVertical: 4,
  },
  line: {
    flex: 1,
    fontSize: 14,
    fontWeight: "600",
  },
  odds: {
    width: 60,
    fontSize: 14,
    fontWeight: "500",
    textAlign: "right",
  },
  score: {
    width: 50,
    fontSize: 14,
    fontWeight: "600",
    textAlign: "right",
  },
});
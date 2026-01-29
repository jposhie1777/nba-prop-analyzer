import { View, Text, StyleSheet } from "react-native";
import { useTheme } from "@/store/useTheme";

export function VendorLadder({ vendorBlock }: { vendorBlock: any }) {
  const { colors } = useTheme();

  return (
    <View style={styles.container}>
      <Text style={styles.vendor}>{vendorBlock.vendor.toUpperCase()}</Text>

      {vendorBlock.rungs.map((r: any, i: number) => (
        <View key={i} style={styles.rung}>
          <Text style={styles.line}>{r.line}</Text>
          <Text style={styles.odds}>
            {r.odds > 0 ? `+${r.odds}` : r.odds}
          </Text>
          <Text style={styles.score}>
            +{r.ladder_score.toFixed(1)}
          </Text>
        </View>
      ))}
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    marginTop: 8,
  },
  vendor: {
    fontWeight: "700",
    marginBottom: 4,
  },
  rung: {
    flexDirection: "row",
    justifyContent: "space-between",
    paddingVertical: 2,
  },
  line: {
    width: 40,
  },
  odds: {
    width: 60,
    textAlign: "right",
  },
  score: {
    width: 60,
    textAlign: "right",
  },
});
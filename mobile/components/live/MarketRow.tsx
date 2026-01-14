// components/live/marketrow
import { View, Text, ScrollView, StyleSheet } from "react-native";
import { LineButton } from "./LineButton";

export function MarketRow({ market, lines }: any) {
  // -----------------------------
  // Split main vs milestone
  // -----------------------------
  const mainLine = lines.find(
    (l: any) => l.line_type === "over_under"
  );

  const milestones = lines
    .filter((l: any) => l.line_type === "milestone")
    .sort((a: any, b: any) => a.line - b.line)
    .slice(0, 3); // keep it clean

  if (!mainLine && milestones.length === 0) {
    return null;
  }

  return (
    <View>
      <Text style={styles.label}>{market}</Text>

      {/* -----------------------------
          MAIN LINE (unchanged UI)
      ----------------------------- */}
      {mainLine && (
        <ScrollView horizontal showsHorizontalScrollIndicator={false}>
          <View style={styles.row}>
            <LineButton
              key={`main-${mainLine.line}`}
              line={mainLine}
              market={market}
            />
          </View>
        </ScrollView>
      )}

      {/* -----------------------------
          MILESTONE LADDER (NEW)
      ----------------------------- */}
      {milestones.length > 0 && (
        <ScrollView
          horizontal
          showsHorizontalScrollIndicator={false}
          style={{ marginTop: 6 }}
        >
          <View style={styles.row}>
            {milestones.map((m: any) => (
              <LineButton
                key={`ms-${m.line}`}
                line={m}
                market={market}
                variant="milestone"   // 👈 optional, see below
              />
            ))}
          </View>
        </ScrollView>
      )}
    </View>
  );
}

const styles = StyleSheet.create({
  label: {
    fontSize: 11,
    fontWeight: "700",
    marginBottom: 4,
  },
  row: {
    flexDirection: "row",
    gap: 8,
  },
});
// components/live/MarketRow
import { View, Text, ScrollView, StyleSheet } from "react-native";
import { LineButton } from "./LineButton";

export function MarketRow({ market, lines }: any) {
  return (
    <View>
      <Text style={styles.label}>{market}</Text>

      <ScrollView horizontal showsHorizontalScrollIndicator={false}>
        <View style={styles.row}>
          {lines.map((l: any) => (
            <LineButton key={l.line} line={l} market={market} />
          ))}
        </View>
      </ScrollView>
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
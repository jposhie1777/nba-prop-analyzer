import { View, Text, StyleSheet } from "react-native";
import { useTheme } from "@/store/useTheme";
import { OddsValue } from "@/types/live";

export function OddsPill({ data }: { data?: OddsValue }) {
  const { colors } = useTheme();

  if (!data) return <View style={styles.empty} />;

  return (
    <View style={[styles.pill, { backgroundColor: colors.surface.elevated }]}>
      <Text style={[styles.line, { color: colors.text.primary }]}>
        {formatLine(data.line)}
      </Text>
      <Text style={[styles.price, { color: colors.text.secondary }]}>
        {formatPrice(data.price)}
      </Text>
    </View>
  );
}

const formatLine = (l: number) => (l > 0 ? `+${l}` : `${l}`);
const formatPrice = (p: number) => (p > 0 ? `+${p}` : `${p}`);

const styles = StyleSheet.create({
  pill: {
    minWidth: 90,
    borderRadius: 8,
    paddingVertical: 6,
    paddingHorizontal: 10,
    alignItems: "center",
  },
  line: {
    fontSize: 13,
    fontWeight: "600",
  },
  price: {
    fontSize: 11,
  },
  empty: {
    minWidth: 90,
  },
});

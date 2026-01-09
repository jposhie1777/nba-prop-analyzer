// components/live/OddsRow
import { View, Text, StyleSheet } from "react-native";
import { useTheme } from "@/store/useTheme";
import { OddsPill } from "./OddsPill";
import { OddsValue } from "@/types/live";

type Props = {
  label: string;
  left?: OddsValue;
  right?: OddsValue;
};

export function OddsRow({ label, left, right }: Props) {
  const { colors } = useTheme();

  if (!left && !right) return null;

  return (
    <View style={styles.row}>
      <Text style={[styles.label, { color: colors.text.muted }]}>
        {label}
      </Text>

      <View style={styles.values}>
        <OddsPill data={left} />
        <OddsPill data={right} />
      </View>
    </View>
  );
}

const styles = StyleSheet.create({
  row: {
    flexDirection: "row",
    alignItems: "center",
    marginTop: 6,
  },
  label: {
    width: 64,
    fontSize: 11,
  },
  values: {
    flex: 1,
    flexDirection: "row",
    justifyContent: "space-between",
  },
});

// components/live/boxscore/StatPill.tsx
import { View, Text, StyleSheet } from "react-native";
import { useTheme } from "@/store/useTheme";

export function StatPill({ value }: { value: number }) {
  const { colors } = useTheme();

  if (value === null || value === undefined) return null;

  const positive = value >= 0;

  return (
    <View
      style={[
        styles.pill,
        {
          backgroundColor: positive
            ? colors.glow.success
            : colors.glow.primary,
        },
      ]}
    >
      <Text style={styles.text}>
        {positive ? "+" : ""}
        {value}
      </Text>
    </View>
  );
}

const styles = StyleSheet.create({
  pill: {
    marginLeft: 6,
    paddingHorizontal: 6,
    paddingVertical: 2,
    borderRadius: 10,
  },
  text: {
    fontSize: 11,
    fontWeight: "600",
  },
});
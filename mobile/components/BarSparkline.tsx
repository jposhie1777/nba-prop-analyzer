// components/BarSparkline.tsx
import { View, StyleSheet } from "react-native";
import { useMemo } from "react";
import { useTheme } from "@/store/useTheme";

type Props = {
  data?: number[];
  height?: number;
};

export function BarSparkline({ data = [], height = 48 }: Props) {
  const colors = useTheme((s) => s.colors);

  const max = useMemo(() => {
    if (!data.length) return 1;
    return Math.max(...data.map((v) => Math.abs(v)));
  }, [data]);

  // ✅ Empty state (renders spacer only)
  if (!data.length) {
    return <View style={[styles.wrap, { height }]} />;
  }

  return (
    <View style={[styles.wrap, { height }]}>
      {data.map((v, i) => {
        const pct = Math.abs(v) / max;
        const barHeight = Math.max(2, pct * height);

        const color =
          v >= 0
            ? colors.accent.success
            : colors.accent.danger;

        return (
          <View key={i} style={styles.barSlot}>
            <View
              style={[
                styles.bar,
                {
                  height: barHeight,
                  backgroundColor: color,
                },
              ]}
            />
          </View>
        );
      })}
    </View>
  );
}

const styles = StyleSheet.create({
  wrap: {
    flexDirection: "row",
    alignItems: "flex-end",
    justifyContent: "center",
    gap: 4,
    marginVertical: 6,
  },
  barSlot: {
    width: 6,
    alignItems: "center",
    justifyContent: "flex-end",
  },
  bar: {
    width: "100%",
    borderRadius: 3,
    opacity: 0.85,
  },
});
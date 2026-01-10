import { View, Text, StyleSheet } from "react-native";
import { useMemo } from "react";
import { useTheme } from "@/store/useTheme";

type Props = {
  data?: number[];
  dates?: string[];
  height?: number;
};

export function BarSparkline({
  data = [],
  dates = [],
  height = 64,
}: Props) {
  const colors = useTheme((s) => s.colors);

  const max = useMemo(() => {
    const vals = data.map(v => Math.abs(v)).filter(Boolean);
    return vals.length ? Math.max(...vals) : 1;
  }, [data]);

  if (!data.length) {
    return <View style={[styles.wrap, { height }]} />;
  }

  return (
    <View style={[styles.wrap, { height }]}>
      {data.map((v, i) => {
        const pct = Math.abs(v) / max;
        const barHeight = Math.max(4, pct * (height - 24));
      
        const color =
          v >= 0
            ? colors.accent.success
            : colors.accent.danger;
      
        const dateLabel =
          dates?.[i] && typeof dates[i] === "string"
            ? dates[i].slice(5)
            : "";
      
        return (
          <View key={i} style={styles.barSlot}>
            {/* VALUE ABOVE */}
            <Text style={styles.value}>{Math.round(v)}</Text>
      
            {/* BAR */}
            <View
              style={[
                styles.bar,
                { height: barHeight, backgroundColor: color },
              ]}
            />
      
            {/* DATE BELOW */}
            <Text style={styles.date}>{dateLabel}</Text>
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
    gap: 8,              // wider spacing
    marginVertical: 10,
  },
  barSlot: {
    width: 16,
    alignItems: "center",
    justifyContent: "flex-end", // 👈 ADD THIS
  },
  value: {
    fontSize: 11,
    fontWeight: "800",
    marginBottom: 4,
    color: "#222",
  },
  bar: {
    width: "100%",
    borderRadius: 4,
    opacity: 0.9,
  },
  date: {
    marginTop: 4,
    fontSize: 9,
    color: "#888",
  },
});
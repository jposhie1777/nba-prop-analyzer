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

  const showEvery =
    data.length <= 5 ? 1 :
    data.length <= 10 ? 2 :
    4;

  return (
    <View style={[styles.wrap, { height }]}>
      {data.map((v, i) => {
        const pct = Math.abs(v) / max;
        const barHeight = Math.max(4, pct * (height - 28));

        const color =
          v >= 0
            ? colors.accent.success
            : colors.accent.danger;

        const showDate = i % showEvery === 0;
        const dateLabel =
          typeof dates?.[i] === "string"
            ? dates[i].slice(5).replace("-", "/")
            : "";

        return (
          <View key={i} style={styles.barSlot}>
            <Text style={styles.value}>{Math.round(v)}</Text>

            <View
              style={[
                styles.bar,
                { height: barHeight, backgroundColor: color },
              ]}
            />

            {showDate ? (
              <Text numberOfLines={1} style={styles.date}>
                {dateLabel}
              </Text>
            ) : (
              <View style={{ height: 11 }} />
            )}
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
    gap: 10,
    marginVertical: 8,
  },

  barSlot: {
    width: 22,              // 👈 prevents date wrapping
    alignItems: "center",
  },

  barArea: {
    height: 44,             // 👈 FIXED baseline
    justifyContent: "flex-end",
    alignItems: "center",
  },

  value: {
    position: "absolute",
    top: -14,
    fontSize: 11,
    fontWeight: "800",
    color: "#222",
  },

  bar: {
    width: 14,
    borderRadius: 4,
    opacity: 0.9,
  },

  date: {
    marginTop: 6,
    fontSize: 9,
    color: "#888",
  },
});
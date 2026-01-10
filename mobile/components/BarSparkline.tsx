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

  // ⬇️ unchanged logic, just clearer intent
  const showEvery =
    data.length <= 5 ? 1 :
    data.length <= 10 ? 2 :
    4;

  return (
    <View style={[styles.wrap, { height }]}>
      {data.map((v, i) => {
        const pct = Math.abs(v) / max;

        // ⬇️ small tweak: bar area is fixed, bars scale inside it
        const barHeight = Math.max(4, pct * styles.barArea.height!);

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
            {/* VALUE ABOVE */}
            <Text style={styles.value}>{Math.round(v)}</Text>

            {/* FIXED BAR BASELINE */}
            <View style={styles.barArea}>
              <View
                style={[
                  styles.bar,
                  { height: barHeight, backgroundColor: color },
                ]}
              />
            </View>

            {/* DATE BELOW (SPARSE) */}
            {showDate ? (
              <Text numberOfLines={1} style={styles.date}>
                {dateLabel}
              </Text>
            ) : (
              <View style={{ height: styles.date.fontSize }} />
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
    gap: 8,                 // ⬅️ slightly tighter = fits L20 better
    marginVertical: 8,
  },

  barSlot: {
    width: 20,              // ⬅️ enough for MM/DD without wrapping
    alignItems: "center",
  },

  barArea: {
    height: 40,             // ⬅️ SINGLE shared baseline
    justifyContent: "flex-end",
    alignItems: "center",
  },

  value: {
    fontSize: 10,
    fontWeight: "800",
    marginBottom: 2,
    color: "#222",
  },

  bar: {
    width: 14,
    borderRadius: 4,
    opacity: 0.9,
  },

  date: {
    marginTop: 4,
    fontSize: 9,
    color: "#888",
  },
});
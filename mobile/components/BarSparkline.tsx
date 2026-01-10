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

  const count = data.length;

  return (
    <View style={[styles.wrap, { height }]}>
      {data.map((v, i) => {
        const pct = Math.abs(v) / max;
        const barHeight = Math.max(4, pct * (height - 28));

        const color =
          v >= 0
            ? colors.accent.success
            : colors.accent.danger;

        // ─────────────────────────────
        // DATE VISIBILITY LOGIC
        // L5  → all
        // L10 → every 2
        // L20 → every 4 + last
        // ─────────────────────────────
        let showDate = true;
        if (count > 15) showDate = i % 4 === 0 || i === count - 1;
        else if (count > 7) showDate = i % 2 === 0;

        const dateLabel =
          showDate && typeof dates?.[i] === "string"
            ? dates[i].slice(5) // "MM-DD"
            : "";

        return (
          <View key={i} style={styles.barSlot}>
            {/* BAR AREA */}
            <View style={styles.barArea}>
              <Text style={styles.value}>{Math.round(v)}</Text>
              <View
                style={[
                  styles.bar,
                  { height: barHeight, backgroundColor: color },
                ]}
              />
            </View>

            {/* DATE */}
            <Text
              style={styles.date}
              numberOfLines={1}
            >
              {dateLabel}
            </Text>
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
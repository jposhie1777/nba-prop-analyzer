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

        // ✅ keep single-line "MM-DD" (no wrapping)
        const dateLabel =
          typeof dates?.[i] === "string"
            ? dates[i].slice(5) // "MM-DD"
            : "";

        return (
          <View key={i} style={styles.barSlot}>
            <Text style={styles.value}>{Math.round(v)}</Text>

            <View style={styles.barArea}>
              <View
                style={[
                  styles.bar,
                  { height: barHeight, backgroundColor: color },
                ]}
              />
            </View>

            {showDate ? (
              <View style={styles.dateWrap}>
                <Text
                  numberOfLines={1}
                  ellipsizeMode="clip"     // ✅ never "..."
                  allowFontScaling={false} // ✅ prevents random truncation
                  style={styles.date}
                >
                  {dateLabel}
                </Text>
              </View>
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

  // ✅ bars can stay narrow, dates no longer depend on this width
  barSlot: {
    width: 18,
    alignItems: "center",
  },

  barArea: {
    height: 44,
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

  // ✅ NEW: date container wider than the bar slot
  dateWrap: {
    width: 34,          // ✅ enough for "12-30"
    alignItems: "center",
    marginTop: 6,
  },

  date: {
    fontSize: 9,
    color: "#888",
  },
});
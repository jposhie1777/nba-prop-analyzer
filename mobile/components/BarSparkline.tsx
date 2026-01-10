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
                  ellipsizeMode="clip"
                  allowFontScaling={false}
                  style={styles.date}
                >
                  {dateLabel}
                </Text>
              </View>
            ) : (
              <View style={styles.dateWrap} />   // 👈 SAME CONTAINER, EMPTY
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
    width: 18,
    alignItems: "center",
  },
  
  barArea: {
    height: 44,                 // 👈 fixed bar baseline
    justifyContent: "flex-end", // 👈 bars sit on same floor
    alignItems: "center",
  },
  
  dateWrap: {
    height: 12,                 // 👈 fixed date baseline
    width: 34,
    alignItems: "center",
    marginTop: 6,
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
    fontSize: 9,
    color: "#888",
  },
});
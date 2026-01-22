// components/sparkline/BaseBarSparkline.tsx
import { View, Text, StyleSheet } from "react-native";
import { useMemo } from "react";
import { useTheme } from "@/store/useTheme";

type Props = {
  data?: number[];
  dates?: string[];

  /* Layout control */
  height: number;
  barWidth: number;
  barGap: number;
  baselineHeight: number;

  /* Display toggles */
  showValues?: boolean;
  showDates?: boolean;

  /* Date density control */
  dateStep?: number | "auto";
};

export function BaseBarSparkline({
  data = [],
  dates = [],

  height,
  barWidth,
  barGap,
  baselineHeight,

  showValues = true,
  showDates = true,
  dateStep = "auto",
}: Props) {
  const colors = useTheme((s) => s.colors);

  /* ==========================
     SCALE NORMALIZATION
  ========================== */
  const max = useMemo(() => {
    const vals = data.map((v) => Math.abs(v)).filter(Boolean);
    return vals.length ? Math.max(...vals) : 1;
  }, [data]);

  /* ==========================
     DATE VISIBILITY LOGIC
  ========================== */
  const dateIndexesToShow = useMemo(() => {
    if (!showDates || !dates.length) return new Set<number>();

    if (dateStep === 0) return new Set<number>();

    if (dateStep !== "auto") {
      return new Set(
        dates.map((_, i) => i).filter((i) => i % dateStep === 0)
      );
    }

    // auto logic
    const step =
      data.length <= 5 ? 1 :
      data.length <= 10 ? 2 :
      5;

    return new Set(
      dates.map((_, i) => i).filter((i) => i % step === 0)
    );
  }, [dates, data.length, dateStep, showDates]);

  /* ==========================
     RENDER
  ========================== */
  return (
    <View
      style={[
        styles.wrap,
        {
          height,
          gap: barGap,
        },
      ]}
    >
      {data.map((v, i) => {
        const pct = Math.abs(v) / max;
        const barHeight = Math.max(
          4,
          pct * (height - baselineHeight)
        );

        const color =
          v >= 0
            ? colors.accent.success
            : colors.accent.danger;

        const showDate = dateIndexesToShow.has(i);

        const dateLabel =
          typeof dates[i] === "string"
            ? dates[i].slice(5)
            : "";

        return (
          <View
            key={i}
            style={[
              styles.barSlot,
              { width: barWidth },
            ]}
          >
            {showValues && (
              <Text
                style={[
                  styles.value,
                  { color: colors.text.primary },
                ]}
              >
                {Math.round(v)}
              </Text>
            )}

            <View
              style={[
                styles.barArea,
                { height: baselineHeight },
              ]}
            >
              <View
                style={[
                  styles.bar,
                  {
                    height: barHeight,
                    width: barWidth,
                    backgroundColor: color,
                  },
                ]}
              />
            </View>

            {showDates && (
              <View style={styles.dateWrap}>
                {showDate && (
                  <Text
                    numberOfLines={1}
                    allowFontScaling={false}
                    style={[
                      styles.date,
                      { color: colors.text.muted },
                    ]}
                  >
                    {dateLabel}
                  </Text>
                )}
              </View>
            )}
          </View>
        );
      })}
    </View>
  );
}

/* ==========================
   STYLES (INTENT-FREE)
========================== */
const styles = StyleSheet.create({
  wrap: {
    flexDirection: "row",
    alignItems: "flex-end",
    justifyContent: "center",
  },

  barSlot: {
    alignItems: "center",
  },

  barArea: {
    justifyContent: "flex-end",
    alignItems: "center",
  },

  value: {
    position: "absolute",
    top: -14,
    fontSize: 11,
    fontWeight: "800",
  },

  bar: {
    borderRadius: 6,
    opacity: 0.9,
  },

  dateWrap: {
    height: 14,
    alignItems: "center",
    marginTop: 6,
  },

  date: {
    fontSize: 9,
    fontWeight: "600",
  },
});
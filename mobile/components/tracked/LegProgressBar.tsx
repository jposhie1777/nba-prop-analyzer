import { View, StyleSheet } from "react-native";
import { useMemo } from "react";
import { useTheme } from "@/store/useTheme";

/* ======================================================
   TYPES
====================================================== */

export type LegStatus =
  | "pending"
  | "winning"
  | "losing"
  | "pushed";

type Props = {
  progress: number; // 0 → 1
  status?: LegStatus; // kept for future use
};

/* ======================================================
   COMPONENT
====================================================== */

export default function LegProgressBar({
  progress,
}: Props) {
  const colors = useTheme((s) => s.colors);
  const styles = useMemo(() => makeStyles(colors), [colors]);

  const clamped = Math.min(Math.max(progress, 0), 1);
  const isComplete = clamped >= 1;

  // 🟡 while filling → 🟢 when complete
  const fillColor = isComplete
    ? colors.accent.success   // green
    : colors.accent.warning;  // yellow

  return (
    <View style={styles.track}>
      <View
        style={[
          styles.fill,
          {
            width: `${Math.round(clamped * 100)}%`,
            backgroundColor: fillColor,
          },
        ]}
      />
    </View>
  );
}

/* ======================================================
   STYLES
====================================================== */

function makeStyles(colors: any) {
  return StyleSheet.create({
    track: {
      height: 6,
      width: "100%",
      backgroundColor: colors.border.subtle,
      borderRadius: 4,
      overflow: "hidden",
    },
    fill: {
      height: "100%",
      borderRadius: 4,
    },
  });
}
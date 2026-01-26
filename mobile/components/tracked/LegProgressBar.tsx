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
  status?: LegStatus;
};

/* ======================================================
   COMPONENT
====================================================== */

export default function LegProgressBar({
  progress,
  status = "pending",
}: Props) {
  const colors = useTheme((s) => s.colors);
  const styles = useMemo(() => makeStyles(colors), [colors]);

  const fillColor =
    status === "winning"
      ? colors.accent.success
      : status === "losing"
      ? colors.accent.danger
      : status === "pushed"
      ? colors.accent.info
      : colors.accent.primary;

  return (
    <View style={styles.track}>
      <View
        style={[
          styles.fill,
          {
            width: `${Math.round(progress * 100)}%`,
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

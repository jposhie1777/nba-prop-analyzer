// components/tracked/HedgeSuggestion.tsx
import { View, Text, StyleSheet, Pressable } from "react-native";
import { useMemo } from "react";
import { useTheme } from "@/store/useTheme";

export type HedgeData = {
  hedge_side: string;
  hedge_line: number;
  hedge_odds: number;
  hedge_book: string;
  risk_level: string;
};

type Props = {
  hedge: HedgeData;
  onPress?: () => void;
};

export default function HedgeSuggestion({ hedge, onPress }: Props) {
  const colors = useTheme((s) => s.colors);
  const styles = useMemo(() => makeStyles(colors), [colors]);

  const isDanger = hedge.risk_level === "danger";
  const oddsDisplay =
    hedge.hedge_odds > 0
      ? `+${hedge.hedge_odds}`
      : String(hedge.hedge_odds);

  return (
    <Pressable
      style={[
        styles.container,
        isDanger && styles.containerDanger,
      ]}
      onPress={onPress}
    >
      <View style={styles.row}>
        <Text style={styles.icon}>🔄</Text>
        <Text style={styles.label}>Hedge:</Text>
        <Text style={styles.value}>
          {hedge.hedge_side.toUpperCase()} {hedge.hedge_line}
        </Text>
        <Text style={styles.odds}>({oddsDisplay})</Text>
        <Text style={styles.book}>{hedge.hedge_book}</Text>
      </View>
    </Pressable>
  );
}

function makeStyles(colors: any) {
  return StyleSheet.create({
    container: {
      backgroundColor: colors.accent.warning + "20",
      borderRadius: 6,
      paddingVertical: 6,
      paddingHorizontal: 8,
      marginTop: 6,
      borderLeftWidth: 3,
      borderLeftColor: colors.accent.warning,
    },

    containerDanger: {
      backgroundColor: colors.accent.danger + "20",
      borderLeftColor: colors.accent.danger,
    },

    row: {
      flexDirection: "row",
      alignItems: "center",
      gap: 4,
      flexWrap: "wrap",
    },

    icon: {
      fontSize: 12,
    },

    label: {
      fontSize: 11,
      fontWeight: "600",
      color: colors.text.secondary,
    },

    value: {
      fontSize: 11,
      fontWeight: "700",
      color: colors.text.primary,
    },

    odds: {
      fontSize: 11,
      fontWeight: "600",
      color: colors.accent.success,
    },

    book: {
      fontSize: 10,
      color: colors.text.muted,
      textTransform: "capitalize",
    },
  });
}

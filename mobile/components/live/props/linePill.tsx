// components/live/props/LinePill.tsx
import { Pressable, Text, StyleSheet, View } from "react-native";
import { useTheme } from "@/store/useTheme";
import { MarketSelection } from "@/types/betting";

type Props = {
  selection: MarketSelection;
  onPress?: (selection: MarketSelection) => void;
  selected?: boolean;
};

export function LinePill({ selection, onPress, selected }: Props) {
  const { colors } = useTheme();

  const odds =
    selection.best.odds > 0
      ? `+${selection.best.odds}`
      : `${selection.best.odds}`;

  return (
    <Pressable
      onPress={() => onPress?.(selection)}
      style={({ pressed }) => [
        styles.pill,
        {
          backgroundColor: selected
            ? colors.accent.primary
            : colors.surface.elevated,
          opacity: pressed ? 0.85 : 1,
        },
      ]}
    >
      <Text
        style={[
          styles.lineText,
          {
            color: selected
              ? colors.text.inverse
              : colors.text.primary,
          },
        ]}
      >
        {selection.outcome === "OVER" ? "O" : "U"} {selection.line}
      </Text>

      <Text
        style={[
          styles.oddsText,
          {
            color: selected
              ? colors.text.inverse
              : colors.text.secondary,
          },
        ]}
      >
        {odds}
      </Text>
    </Pressable>
  );
}

const styles = StyleSheet.create({
  pill: {
    minWidth: 72,
    borderRadius: 8,
    paddingVertical: 6,
    paddingHorizontal: 10,
    alignItems: "center",
    justifyContent: "center",
  },
  lineText: {
    fontSize: 12,
    fontWeight: "700",
  },
  oddsText: {
    fontSize: 11,
    marginTop: 2,
  },
});
// components/live/props/LinePill.tsx
import { Pressable, Text, StyleSheet } from "react-native";
import { useTheme } from "@/store/useTheme";
import { MarketSelection } from "@/types/betting";
import { useBetsStore } from "@/store/useBetsStore";

type Props = {
  selection: MarketSelection;
};

export function LinePill({ selection }: Props) {
  const { colors } = useTheme();
  const toggleBet = useBetsStore((s) => s.toggleBet);
  const isSelected = useBetsStore((s) =>
    s.isSelected(selection.selectionId)
  );

  const odds =
    selection.best.odds > 0
      ? `+${selection.best.odds}`
      : `${selection.best.odds}`;

  return (
    <Pressable
      onPress={() => toggleBet(selection)}
      style={({ pressed }) => [
        styles.pill,
        {
          backgroundColor: isSelected
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
            color: isSelected
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
            color: isSelected
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
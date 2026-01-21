// components/live/OddsButton.tsx
import { Pressable, Text, StyleSheet } from "react-native";
import { useTheme } from "@/store/useTheme";
import { useSavedBets } from "@/store/useSavedBets";
import { Bet } from "@/types/bet";

export function OddsButton({ bet }: { bet: Bet }) {
  const { colors } = useTheme();
  const { bets, addBet, removeBet } = useBetslip();

  if (bet.odds == null) return null;

  const isSelected = bets.some((b) => b.id === bet.id);

  const handlePress = () => {
    if (isSelected) {
      removeBet(bet.id);
    } else {
      addBet(bet);
    }
  };

  return (
    <Pressable
      onPress={handlePress}
      style={({ pressed }) => [
        styles.btn,
        {
          backgroundColor: isSelected
            ? colors.accent.primary + "22"
            : pressed
            ? colors.surface.subtle
            : colors.surface.card,

          borderColor: isSelected
            ? colors.accent.primary
            : colors.border.subtle,
        },
      ]}
    >
      <Text
        style={[
          styles.label,
          {
            color: isSelected
              ? colors.accent.primary
              : colors.text.primary,
          },
        ]}
      >
        {bet.display?.title ?? bet.label}
      </Text>

      <Text style={[styles.odds, { color: colors.text.muted }]}>
        {bet.odds > 0 ? `+${bet.odds}` : bet.odds}
      </Text>
    </Pressable>
  );
}

const styles = StyleSheet.create({
  btn: {
    borderWidth: 1,
    borderRadius: 10,
    paddingVertical: 6,
    paddingHorizontal: 10,
    minWidth: 72,
    alignItems: "center",
  },
  label: {
    fontSize: 11,
    fontWeight: "700",
    textAlign: "center",
  },
  odds: {
    fontSize: 10,
    marginTop: 2,
  },
});
// components/bets/BetButton.tsx
import { Pressable, Text, StyleSheet } from "react-native";
import { Bet } from "@/types/bet";
import { useBetslip } from "@/store/useBetslip";
import { useTheme } from "@/store/useTheme";

export function BetButton({ bet }: { bet: Bet }) {
  const { colors } = useTheme();
  const addBet = useBetslip(s => s.addBet);

  return (
    <Pressable
      style={[styles.button, { borderColor: colors.border }]}
      onPress={() => addBet(bet)}
    >
      <Text style={{ color: colors.text, fontWeight: "600" }}>
        {bet.display.title}
      </Text>
      <Text style={{ color: colors.muted }}>
        {bet.odds > 0 ? `+${bet.odds}` : bet.odds}
      </Text>
    </Pressable>
  );
}

const styles = StyleSheet.create({
  button: {
    paddingVertical: 10,
    paddingHorizontal: 14,
    borderRadius: 10,
    borderWidth: 1,
    alignItems: "center",
  },
});
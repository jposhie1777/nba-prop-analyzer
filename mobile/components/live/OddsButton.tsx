// components/live/OddsButton.tsx
import { Pressable, Text, StyleSheet } from "react-native";
import { useTheme } from "@/store/useTheme";
import { useBetslip } from "@/store/useBetslip";
import { Bet } from "@/types/bet";

export function OddsButton({ bet }: { bet: Bet }) {
  const { colors } = useTheme();
  const addBet = useBetslip((s) => s.addBet);

  if (bet.odds === null) return null;

  return (
    <Pressable
      onPress={() => addBet(bet)}
      style={[
        styles.btn,
        {
          backgroundColor: colors.surface.card,
          borderColor: colors.border.subtle,
        },
      ]}
    >
      <Text style={[styles.label, { color: colors.text.primary }]}>
        {bet.display.title}
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
  },
  odds: {
    fontSize: 10,
  },
});
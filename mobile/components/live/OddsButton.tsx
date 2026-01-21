// components/live/OddsButton.tsx
import { Pressable, Text, StyleSheet } from "react-native";
import { useTheme } from "@/store/useTheme";
import { useSavedBets } from "@/store/useSavedBets";
import { SavedBet } from "@/store/useSavedBets";

/* ============================
   LABEL FORMATTER (ADJUSTMENT)
============================ */

function formatBetLabel(bet: SavedBet) {
  // Game bets
  if (bet.betType === "game") {
    if (bet.side === "over") return `OVER ${bet.line}`;
    if (bet.side === "under") return `UNDER ${bet.line}`;
    return `${bet.side.toUpperCase()} ${bet.line}`;
  }

  // Player props
  if (bet.side === "milestone") {
    return `${bet.line}+`;
  }

  return `${bet.side.toUpperCase()} ${bet.line}`;
}

/* ============================
   COMPONENT
============================ */

export function OddsButton({ bet }: { bet: SavedBet }) {
  const { colors } = useTheme();
  const toggleSave = useSavedBets((s) => s.toggleSave);
  const savedIds = useSavedBets((s) => s.savedIds);

  if (bet.odds == null) return null;

  const isSelected = savedIds.has(bet.id);

  return (
    <Pressable
      onPress={() => toggleSave(bet)}
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
        {formatBetLabel(bet)}
      </Text>

      <Text style={[styles.odds, { color: colors.text.muted }]}>
        {bet.odds > 0 ? `+${bet.odds}` : bet.odds}
      </Text>
    </Pressable>
  );
}

/* ============================
   STYLES (UNCHANGED)
============================ */

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
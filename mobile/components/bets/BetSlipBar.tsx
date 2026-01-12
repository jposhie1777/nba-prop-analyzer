// components/bets/BetSlipBar.tsx
import { View, Text, Pressable, StyleSheet } from "react-native";
import { useTheme } from "@/store/useTheme";
import { useBetsStore } from "@/store/useBetsStore";

export function BetSlipBar() {
  const { colors } = useTheme();

  const bets = useBetsStore((s) => s.betsById);
  const betCount = Object.keys(bets).length;

  if (betCount === 0) return null;

  return (
    <View
      style={[
        styles.container,
        { backgroundColor: colors.surface.elevated },
      ]}
    >
      {/* LEFT: COUNT */}
      <View style={styles.left}>
        <Text
          style={[
            styles.count,
            { color: colors.text.primary },
          ]}
        >
          {betCount}
        </Text>
        <Text
          style={[
            styles.label,
            { color: colors.text.secondary },
          ]}
        >
          Bet{betCount > 1 ? "s" : ""} selected
        </Text>
      </View>

      {/* RIGHT: CTA */}
      <Pressable
        onPress={() => {
          // 🔜 open bet slip modal
          console.log("OPEN BET SLIP", bets);
        }}
        style={[
          styles.cta,
          { backgroundColor: colors.accent.primary },
        ]}
      >
        <Text
          style={[
            styles.ctaText,
            { color: colors.text.inverse },
          ]}
        >
          View Bets
        </Text>
      </Pressable>
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    position: "absolute",
    left: 12,
    right: 12,
    bottom: 12,

    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",

    paddingHorizontal: 14,
    paddingVertical: 12,
    borderRadius: 14,

    // subtle elevation
    shadowColor: "#000",
    shadowOpacity: 0.15,
    shadowRadius: 8,
    shadowOffset: { width: 0, height: 4 },
    elevation: 6,
  },

  left: {
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
  },

  count: {
    fontSize: 18,
    fontWeight: "800",
  },

  label: {
    fontSize: 13,
    fontWeight: "600",
  },

  cta: {
    paddingHorizontal: 16,
    paddingVertical: 8,
    borderRadius: 999,
  },

  ctaText: {
    fontSize: 13,
    fontWeight: "800",
    letterSpacing: 0.5,
  },
});
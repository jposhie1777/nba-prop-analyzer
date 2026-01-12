// components/bets/BetSlipBar.tsx
import { View, Text, Pressable, StyleSheet } from "react-native";
import { useTheme } from "@/store/useTheme";
import { useBetsStore } from "@/store/useBetsStore";
import { useState } from "react";
import { BetSlipModal } from "./BetSlipModal";

export function BetSlipBar() {
  const { colors } = useTheme();
  const [open, setOpen] = useState(false);

  const bets = useBetsStore((s) => s.betsById);
  const betCount = Object.keys(bets).length;

  if (betCount === 0) return null;

  return (
    <>
      <View
        style={[
          styles.container,
          { backgroundColor: colors.surface.elevated },
        ]}
      >
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

        <Pressable
          onPress={() => setOpen(true)}
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

      <BetSlipModal visible={open} onClose={() => setOpen(false)} />
    </>
  );
}
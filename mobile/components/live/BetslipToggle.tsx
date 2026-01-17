import { Pressable, Text, StyleSheet } from "react-native";
import { useSavedBets } from "@/store/useSavedBets";
import { useTheme } from "@/store/useTheme";

export function BetslipToggle({ onPress }: { onPress: () => void }) {
  const { savedIds } = useSavedBets();
  const { colors } = useTheme();

  const count = savedIds.size;
  if (count === 0) return null;

  return (
    <Pressable
      onPress={onPress}
      style={[
        styles.pill,
        { backgroundColor: colors.accent.primary },
      ]}
    >
      <Text style={styles.text}>
        Betslip · {count}
      </Text>
    </Pressable>
  );
}

const styles = StyleSheet.create({
  pill: {
    position: "absolute",
    bottom: 20,
    alignSelf: "center",
    paddingHorizontal: 16,
    paddingVertical: 10,
    borderRadius: 999,
    elevation: 6,
  },
  text: {
    color: "white",
    fontWeight: "800",
  },
});
import { Pressable, Text, StyleSheet } from "react-native";
import { useTheme } from "@/store/useTheme";

export function OddsButton({
  label,
  odds,
  onPress,
}: {
  label: string;
  odds: number | null;
  onPress?: () => void;
}) {
  const { colors } = useTheme();

  if (odds === null) return null;

  return (
    <Pressable
      onPress={onPress}
      style={[
        styles.btn,
        {
          backgroundColor: colors.surface.card,
          borderColor: colors.border.subtle,
        },
      ]}
    >
      <Text style={[styles.label, { color: colors.text.primary }]}>
        {label}
      </Text>
      <Text style={[styles.odds, { color: colors.text.muted }]}>
        {odds > 0 ? `+${odds}` : odds}
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
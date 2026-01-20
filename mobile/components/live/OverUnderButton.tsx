// components/live/OverUnderButton.tsx
import { Pressable, Text, StyleSheet } from "react-native";
import { useTheme } from "@/store/useTheme";

type Props = {
  side: "over" | "under";
  line: number;
  odds: number | null;
  disabled?: boolean;
  isSelected?: boolean;
  onPress?: () => void;
};

export function OverUnderButton({
  side,
  line,
  odds,
  disabled = false,
  isSelected = false,
  onPress,
}: Props) {
  const { colors } = useTheme();
  const isOver = side === "over";

  const accentColor = isOver ? colors.success : colors.danger;

  return (
    <Pressable
      disabled={disabled}
      onPress={onPress}
      style={({ pressed }) => [
        styles.btn,
        {
          borderColor: disabled
            ? colors.border.subtle
            : isSelected
            ? accentColor
            : accentColor,

          backgroundColor: isSelected
            ? accentColor + "22"
            : pressed
            ? colors.surface.subtle
            : "transparent",

          opacity: disabled ? 0.4 : 1,
        },
      ]}
    >
      {/* SIDE LABEL */}
      <Text
        style={[
          styles.side,
          {
            color: accentColor,
            opacity: disabled ? 0.6 : 1,
          },
        ]}
      >
        {isOver ? "Over" : "Under"}
      </Text>

      {/* LINE */}
      <Text style={[styles.line, { color: colors.text.primary }]}>
        {line}
      </Text>

      {/* ODDS */}
      <Text style={[styles.odds, { color: colors.text.muted }]}>
        {odds != null ? (odds > 0 ? `+${odds}` : odds) : "—"}
      </Text>
    </Pressable>
  );
}

const styles = StyleSheet.create({
  container: {
    minWidth: 84,
    paddingVertical: 6,
    paddingHorizontal: 10,
    borderRadius: 10,
    borderWidth: 1,
  },
  
  line: {
    fontSize: 12,
    fontWeight: "600",
  },
  
  odds: {
    fontSize: 11,
    opacity: 0.75,
  },
  
  side: {
    fontSize: 10,
    opacity: 0.6,
  },
});
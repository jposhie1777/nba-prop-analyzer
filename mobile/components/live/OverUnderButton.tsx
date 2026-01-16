import { Pressable, Text, View } from "react-native";
import { useTheme } from "@/store/useTheme";

type Props = {
  side: "over" | "under";
  line: number;
  odds: number | null;
  disabled?: boolean;
  onPress?: () => void;
};

export function OverUnderButton({
  side,
  line,
  odds,
  disabled,
  onPress,
}: Props) {
  const { colors } = useTheme();
  const isOver = side === "over";

  return (
    <Pressable
      disabled={disabled}
      onPress={onPress}
      style={({ pressed }) => ({
        flex: 1,
        borderWidth: 1,
        borderRadius: 8,
        paddingVertical: 6,     // 👈 smaller height
        paddingHorizontal: 6,
        alignItems: "center",
        justifyContent: "center",
        borderColor: disabled
          ? colors.border.subtle
          : isOver
          ? colors.success
          : colors.danger,
        backgroundColor: pressed
          ? colors.background.subtle
          : "transparent",
        opacity: disabled ? 0.4 : 1,
      })}
    >
      {/* Over / Under label */}
      <Text
        style={{
          fontSize: 10,        // 👈 smaller
          fontWeight: "600",
          marginBottom: 2,
          color: isOver ? colors.success : colors.danger,
        }}
      >
        {isOver ? "Over" : "Under"}
      </Text>

      {/* Line */}
      <Text
        style={{
          fontSize: 15,        // 👈 compact but readable
          fontWeight: "700",
          lineHeight: 18,
          color: colors.text.primary,
        }}
      >
        {line}
      </Text>

      {/* Odds */}
      <Text
        style={{
          fontSize: 11,
          marginTop: 1,
          color: colors.text.muted,
        }}
      >
        {odds != null ? (odds > 0 ? `+${odds}` : odds) : "—"}
      </Text>
    </Pressable>
  );
}
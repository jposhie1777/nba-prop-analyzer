import { Pressable, Text, View } from "react-native";
import { useTheme } from "@/store/useTheme";

type Props = {
  side: "over" | "under";
  line: number;
  odds: number | null;
  onPress?: () => void;
};

export function OverUnderButton({ side, line, odds, onPress }: Props) {
  const { colors } = useTheme();
  const isOver = side === "over";

  return (
    <Pressable
      onPress={onPress}
      style={({ pressed }) => ({
        flex: 1,
        borderWidth: 1.5,
        borderColor: isOver ? colors.success : colors.danger,
        borderRadius: 10,
        paddingVertical: 8,
        alignItems: "center",
        opacity: pressed ? 0.7 : 1,
      })}
    >
      {/* Label */}
      <Text
        style={{
          fontSize: 12,
          fontWeight: "600",
          color: isOver ? colors.success : colors.danger,
          marginBottom: 4,
        }}
      >
        {isOver ? "Over" : "Under"}
      </Text>

      {/* Line */}
      <Text
        style={{
          fontSize: 16,
          fontWeight: "700",
          color: colors.text.primary,
        }}
      >
        {line}
      </Text>

      {/* Odds */}
      <Text
        style={{
          fontSize: 13,
          marginTop: 2,
          color: colors.text.muted,
        }}
      >
        {odds !== null ? (odds > 0 ? `+${odds}` : odds) : "—"}
      </Text>
    </Pressable>
  );
}
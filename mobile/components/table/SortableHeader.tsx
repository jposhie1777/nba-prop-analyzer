// components/table/SortableHeader.tsx
import { Pressable, Text, View } from "react-native";
import { useTheme } from "@/store/useTheme";

export function SortableHeader({
  label,
  active,
  direction,
  onPress,
  width = 70,
}: {
  label: string;
  active: boolean;
  direction?: "asc" | "desc";
  onPress: () => void;
  width?: number;
}) {
  const { colors } = useTheme();

  return (
    <Pressable onPress={onPress} style={{ width }}>
      <Text
        style={{
          fontWeight: "600",
          color: active ? colors.accent.primary : colors.text.secondary,
        }}
      >
        {label}
        {active ? (direction === "asc" ? " ▲" : " ▼") : ""}
      </Text>
    </Pressable>
  );
}
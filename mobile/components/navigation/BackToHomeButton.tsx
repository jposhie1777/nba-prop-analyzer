import { Pressable, StyleSheet, Text } from "react-native";
import { useRouter } from "expo-router";

import { useTheme } from "@/store/useTheme";

type Props = {
  label?: string;
};

export function BackToHomeButton({ label = "← Back" }: Props) {
  const router = useRouter();
  const { colors } = useTheme();

  return (
    <Pressable
      onPress={() => router.push("/(tabs)/home")}
      style={[styles.actionButton, { borderColor: colors.border.subtle }]}
    >
      <Text style={styles.actionText}>{label}</Text>
    </Pressable>
  );
}

const styles = StyleSheet.create({
  actionButton: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 12,
    backgroundColor: "#0B1529",
    paddingHorizontal: 12,
    paddingVertical: 8,
  },
  actionText: { color: "#E9F2FF", fontWeight: "700", fontSize: 12 },
});

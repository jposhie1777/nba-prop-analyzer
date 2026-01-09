// components/live/BoxScore
import { View, Text, StyleSheet } from "react-native";
import { useTheme } from "@/store/useTheme";

export function BoxScoreStub() {
  const { colors } = useTheme();

  return (
    <View style={styles.container}>
      <Text style={[styles.label, { color: colors.text.secondary }]}>
        Box Score
      </Text>
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    paddingVertical: 6,
  },
  label: {
    fontSize: 12,
    fontWeight: "600",
  },
});

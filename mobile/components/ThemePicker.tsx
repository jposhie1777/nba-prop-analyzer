// components/ThemePicker.tsx
import { View, Text, StyleSheet, Pressable } from "react-native";
import { useTheme } from "@/store/useTheme";
import { themeMeta } from "@/theme/meta";

export default function ThemePicker() {
  const { theme, setTheme, colors } = useTheme();

  return (
    <View style={[styles.container, { backgroundColor: colors.surface.card }]}>
      <Text style={[styles.title, { color: colors.text.primary }]}>
        Theme
      </Text>

      {Object.entries(themeMeta).map(([key, meta]) => {
        const active = theme === key;

        return (
          <Pressable
            key={key}
            onPress={() => setTheme(key as any)}
            style={[
              styles.row,
              {
                backgroundColor: active
                  ? colors.surface.elevated
                  : "transparent",
                borderColor: active
                  ? colors.accent.primary
                  : colors.border.subtle,
              },
            ]}
          >
            <View
              style={[
                styles.preview,
                { backgroundColor: meta.preview },
              ]}
            />

            <Text
              style={[
                styles.label,
                {
                  color: active
                    ? colors.text.primary
                    : colors.text.secondary,
                },
              ]}
            >
              {meta.label}
            </Text>

            {active && (
              <Text style={{ color: colors.accent.primary }}>✓</Text>
            )}
          </Pressable>
        );
      })}
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    borderRadius: 16,
    padding: 16,
    gap: 10,
  },
  title: {
    fontSize: 16,
    fontWeight: "800",
    marginBottom: 6,
  },
  row: {
    flexDirection: "row",
    alignItems: "center",
    gap: 12,
    padding: 12,
    borderRadius: 12,
    borderWidth: 1,
  },
  preview: {
    width: 28,
    height: 28,
    borderRadius: 8,
  },
  label: {
    flex: 1,
    fontSize: 14,
    fontWeight: "700",
  },
});
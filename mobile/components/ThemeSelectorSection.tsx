import { useState } from "react";
import { View, Text, StyleSheet, Pressable } from "react-native";
import { useTheme } from "@/store/useTheme";
import { themeMeta } from "@/theme/meta";
import ThemePicker from "@/components/ThemePicker";

type Props = {
  title?: string;
};

export default function ThemeSelectorSection({ title = "Theme" }: Props) {
  const [open, setOpen] = useState(false);
  const { theme, colors } = useTheme();
  const label = themeMeta[theme]?.label ?? theme;

  return (
    <View style={styles.wrapper}>
      <View style={styles.header}>
        <Text style={[styles.title, { color: colors.text.primary }]}
        >
          {title}
        </Text>
        <Pressable
          onPress={() => setOpen((prev) => !prev)}
          style={({ pressed }) => [
            styles.button,
            {
              backgroundColor: colors.surface.card,
              borderColor: colors.border.subtle,
              opacity: pressed ? 0.85 : 1,
            },
          ]}
        >
          <Text style={[styles.buttonText, { color: colors.text.primary }]}
          >
            {open ? "Hide" : "Choose"} • {label}
          </Text>
        </Pressable>
      </View>
      {open ? (
        <View style={styles.picker}>
          <ThemePicker />
        </View>
      ) : null}
    </View>
  );
}

const styles = StyleSheet.create({
  wrapper: {
    marginBottom: 16,
  },
  header: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    gap: 12,
  },
  title: {
    fontSize: 16,
    fontWeight: "800",
  },
  button: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 999,
    paddingHorizontal: 14,
    paddingVertical: 8,
  },
  buttonText: {
    fontSize: 12,
    fontWeight: "700",
  },
  picker: {
    marginTop: 12,
  },
});

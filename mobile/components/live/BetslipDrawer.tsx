import { View, Text, Pressable, StyleSheet, Animated } from "react-native";
import { useEffect, useRef } from "react";
import { useSavedBets } from "@/store/useSavedBets";
import { useTheme } from "@/store/useTheme";

const DRAWER_HEIGHT = 260;

export function BetslipDrawer({
  open,
  onClose,
}: {
  open: boolean;
  onClose: () => void;
}) {
  const { savedIds, clearAll } = useSavedBets();
  const { colors } = useTheme();

  const translateY = useRef(
    new Animated.Value(DRAWER_HEIGHT)
  ).current;

  useEffect(() => {
    Animated.spring(translateY, {
      toValue: open ? 0 : DRAWER_HEIGHT,
      useNativeDriver: true,
    }).start();
  }, [open]);

  if (savedIds.size === 0) return null;

  const copyForGambly = () => {
    const text = Array.from(savedIds)
      .map((id) => `• ${id}`)
      .join("\n");

    navigator.clipboard?.writeText?.(text);
  };

  return (
    <Animated.View
      style={[
        styles.drawer,
        {
          backgroundColor: colors.surface.card,
          transform: [{ translateY }],
        },
      ]}
    >
      <View style={styles.header}>
        <Text style={styles.title}>
          Betslip ({savedIds.size})
        </Text>

        <Pressable onPress={onClose}>
          <Text style={styles.close}>Close</Text>
        </Pressable>
      </View>

      <View style={styles.actions}>
        <Pressable
          style={[
            styles.primary,
            { backgroundColor: colors.accent.primary },
          ]}
          onPress={copyForGambly}
        >
          <Text style={styles.primaryText}>
            Copy for Gambly
          </Text>
        </Pressable>

        <Pressable onPress={clearAll}>
          <Text style={styles.clear}>Clear</Text>
        </Pressable>
      </View>
    </Animated.View>
  );
}

const styles = StyleSheet.create({
  drawer: {
    position: "absolute",
    bottom: 0,
    left: 0,
    right: 0,
    height: DRAWER_HEIGHT,
    borderTopLeftRadius: 20,
    borderTopRightRadius: 20,
    padding: 14,
    elevation: 12,
  },
  header: {
    flexDirection: "row",
    justifyContent: "space-between",
    marginBottom: 12,
  },
  title: {
    fontWeight: "800",
    fontSize: 16,
  },
  close: {
    fontWeight: "600",
  },
  actions: {
    marginTop: "auto",
    gap: 12,
  },
  primary: {
    padding: 12,
    borderRadius: 12,
    alignItems: "center",
  },
  primaryText: {
    color: "white",
    fontWeight: "800",
  },
  clear: {
    textAlign: "center",
    fontWeight: "600",
  },
});
// components/live/BetslipDrawer.tsx
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

  return (
    <Animated.View
      style={[
        styles.drawer,
        {
          backgroundColor: colors.surface.card,
          borderTopColor: colors.border.subtle,
          transform: [{ translateY }],
        },
      ]}
    >
      {/* Header */}
      <View style={styles.header}>
        <Text
          style={[
            styles.title,
            { color: colors.text.primary },
          ]}
        >
          Betslip ({savedIds.size})
        </Text>

        <Pressable onPress={onClose}>
          <Text
            style={{
              color: colors.text.muted,
              fontWeight: "600",
            }}
          >
            Close
          </Text>
        </Pressable>
      </View>

      {/* Placeholder for bet rows (next step) */}
      <View style={styles.body}>
        <Text
          style={{ color: colors.text.muted }}
        >
          Bets ready to copy
        </Text>
      </View>

      {/* Actions */}
      <View style={styles.actions}>
        <Pressable
          style={[
            styles.primary,
            {
              backgroundColor: colors.accent.primary,
              shadowColor: colors.accent.primary,
            },
          ]}
        >
          <Text
            style={{
              color: colors.text.inverse,
              fontWeight: "800",
            }}
          >
            Copy for Gambly
          </Text>
        </Pressable>

        <Pressable onPress={clearAll}>
          <Text
            style={{
              color: colors.accent.danger,
              fontWeight: "600",
              textAlign: "center",
            }}
          >
            Clear all
          </Text>
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
    borderTopWidth: 1,
    elevation: 12,
  },
  header: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    marginBottom: 8,
  },
  title: {
    fontSize: 16,
    fontWeight: "800",
  },
  body: {
    flex: 1,
    justifyContent: "center",
  },
  actions: {
    gap: 10,
  },
  primary: {
    paddingVertical: 12,
    borderRadius: 12,
    alignItems: "center",
    shadowOpacity: 0.18,
    shadowRadius: 8,
    shadowOffset: { width: 0, height: 4 },
  },
});
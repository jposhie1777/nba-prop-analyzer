// components/prop/PropBetslipDrawer.tsx
import {
  View,
  Text,
  Pressable,
  StyleSheet,
  Linking,
} from "react-native";
import { useMemo } from "react";
import * as Clipboard from "expo-clipboard";
import * as Haptics from "expo-haptics";

import { usePropBetslip } from "@/store/usePropBetslip";
import { useTheme } from "@/store/useTheme";

const GAMBLY_URL = "https://www.gambly.com/gambly-bot";

export function PropBetslipDrawer() {
  const colors = useTheme((s) => s.colors);
  const { items, remove, clear } = usePropBetslip();

  const text = useMemo(
    () =>
      items
        .map(
          (b) =>
            `${b.player} ${b.market} ${b.side === "under" ? "U" : "O"} ${b.line} ${b.odds}`
        )
        .join("\n"),
    [items]
  );

  if (!items.length) return null;

  return (
    <View style={[styles.wrap, { backgroundColor: colors.surface.card }]}>
      {items.map((b) => (
        <View key={b.id} style={styles.row}>
          <Text style={styles.label}>
            {b.player} {b.market} {b.side === "under" ? "U" : "O"} {b.line}
          </Text>
          <Text style={styles.odds}>{b.odds}</Text>
          <Pressable onPress={() => remove(b.id)}>
            <Text style={styles.remove}>✕</Text>
          </Pressable>
        </View>
      ))}

      <View style={styles.actions}>
        <Pressable
          onPress={async () => {
            await Clipboard.setStringAsync(text);
            Haptics.notificationAsync(
              Haptics.NotificationFeedbackType.Success
            );
          }}
          style={[styles.btn, { backgroundColor: colors.surface.elevated }]}
        >
          <Text style={styles.btnText}>Copy Bets</Text>
        </Pressable>

        <Pressable
          onPress={() => Linking.openURL(GAMBLY_URL)}
          style={[styles.btn, { backgroundColor: colors.accent.primary }]}
        >
          <Text style={styles.btnText}>Open Gambly</Text>
        </Pressable>
      </View>
    </View>
  );
}

const styles = StyleSheet.create({
  wrap: {
    position: "absolute",
    bottom: 0,
    left: 0,
    right: 0,
    borderTopWidth: 1,
    padding: 12,
  },
  row: {
    flexDirection: "row",
    alignItems: "center",
    marginBottom: 6,
  },
  label: { flex: 1, fontWeight: "700" },
  odds: { width: 50, textAlign: "right", fontWeight: "800" },
  remove: { marginLeft: 8, fontWeight: "900" },
  actions: { flexDirection: "row", gap: 10, marginTop: 8 },
  btn: {
    flex: 1,
    paddingVertical: 12,
    borderRadius: 14,
    alignItems: "center",
  },
  btnText: { fontWeight: "900" },
});
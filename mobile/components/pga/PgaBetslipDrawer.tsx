import { Linking, Pressable, ScrollView, StyleSheet, Text, View } from "react-native";
import { useMemo, useState } from "react";
import * as Clipboard from "expo-clipboard";

import { useTheme } from "@/store/useTheme";
import { usePgaBetslip } from "@/store/usePgaBetslip";
import { usePgaBetslipDrawer } from "@/store/usePgaBetslipDrawer";

const GAMBLY_URL = "https://www.gambly.com/gambly-bot";

export function PgaBetslipDrawer() {
  const { colors } = useTheme();
  const { items, remove, clear } = usePgaBetslip();
  const { isOpen, close } = usePgaBetslipDrawer();
  const [expanded, setExpanded] = useState(true);

  const text = useMemo(
    () =>
      items
        .map((item, idx) => {
          const opponents = item.groupPlayers
            .filter((p) => p !== item.playerLastName)
            .join(", ");
          const details = [
            item.roundNumber ? `R${item.roundNumber}` : null,
            item.teeTime,
          ]
            .filter(Boolean)
            .join(" • ");
          const suffix = details ? ` (${details})` : "";
          return `${idx + 1}. ${item.playerLastName} to beat group (${opponents})${suffix}`;
        })
        .join("\n"),
    [items]
  );

  if (!items.length && !isOpen) return null;

  return (
    <View
      style={[
        styles.wrap,
        {
          backgroundColor: colors.surface.card,
          borderTopColor: colors.border.subtle,
          transform: [{ translateY: isOpen ? 0 : 250 }],
        },
      ]}
    >
      <Pressable style={styles.header} onPress={() => setExpanded((v) => !v)}>
        <Text style={[styles.title, { color: colors.text.primary }]}>
          PGA Betslip ({items.length})
        </Text>
        <Text style={[styles.chevron, { color: colors.text.muted }]}>
          {expanded ? "▼" : "▲"}
        </Text>
      </Pressable>

      {expanded && (
        <>
          <ScrollView style={styles.list} showsVerticalScrollIndicator={false}>
            {items.map((item) => {
              const opponents = item.groupPlayers
                .filter((p) => p !== item.playerLastName)
                .join(", ");
              return (
                <View
                  key={item.id}
                  style={[styles.row, { borderBottomColor: colors.border.subtle }]}
                >
                  <View style={styles.rowTextWrap}>
                    <Text
                      style={[styles.rowTitle, { color: colors.text.primary }]}
                      numberOfLines={2}
                    >
                      {item.playerLastName} to beat group
                    </Text>
                    <Text
                      style={[styles.rowMeta, { color: colors.text.muted }]}
                      numberOfLines={1}
                    >
                      vs {opponents}
                      {item.roundNumber ? ` • R${item.roundNumber}` : ""}
                    </Text>
                  </View>
                  <Pressable onPress={() => remove(item.id)}>
                    <Text style={[styles.remove, { color: "#ef4444" }]}>Remove</Text>
                  </Pressable>
                </View>
              );
            })}
          </ScrollView>

          <View style={styles.actions}>
            <Pressable
              style={[styles.btn, { backgroundColor: colors.surface.cardSoft }]}
              onPress={async () => {
                if (!text) return;
                await Clipboard.setStringAsync(text);
              }}
            >
              <Text style={[styles.btnText, { color: colors.text.primary }]}>
                Copy for Gambly
              </Text>
            </Pressable>
            <Pressable
              style={[styles.btn, { backgroundColor: colors.surface.cardSoft }]}
              onPress={() => Linking.openURL(GAMBLY_URL)}
            >
              <Text style={[styles.btnText, { color: colors.text.primary }]}>
                Open Gambly
              </Text>
            </Pressable>
          </View>

          <View style={styles.actions}>
            <Pressable
              style={[styles.secondaryBtn, { borderColor: colors.border.subtle }]}
              onPress={() => {
                clear();
                close();
              }}
            >
              <Text style={[styles.secondaryText, { color: colors.text.muted }]}>
                Clear
              </Text>
            </Pressable>
          </View>
        </>
      )}
    </View>
  );
}

const styles = StyleSheet.create({
  wrap: {
    position: "absolute",
    bottom: 0,
    left: 0,
    right: 0,
    borderTopWidth: StyleSheet.hairlineWidth,
    borderTopLeftRadius: 16,
    borderTopRightRadius: 16,
    paddingHorizontal: 12,
    paddingTop: 10,
    paddingBottom: 16,
    zIndex: 100,
  },
  header: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    paddingBottom: 8,
  },
  title: { fontSize: 16, fontWeight: "800" },
  chevron: { fontSize: 13, fontWeight: "700" },
  list: { maxHeight: 180 },
  row: {
    paddingVertical: 10,
    borderBottomWidth: StyleSheet.hairlineWidth,
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    gap: 10,
  },
  rowTextWrap: { flex: 1 },
  rowTitle: { fontSize: 13, fontWeight: "700" },
  rowMeta: { fontSize: 12, marginTop: 3 },
  remove: { fontSize: 12, fontWeight: "700" },
  actions: { flexDirection: "row", gap: 8, marginTop: 10 },
  btn: { flex: 1, borderRadius: 10, paddingVertical: 10, alignItems: "center" },
  btnText: { fontSize: 13, fontWeight: "800" },
  secondaryBtn: {
    flex: 1,
    borderRadius: 10,
    paddingVertical: 10,
    alignItems: "center",
    borderWidth: StyleSheet.hairlineWidth,
  },
  secondaryText: { fontSize: 13, fontWeight: "700" },
});

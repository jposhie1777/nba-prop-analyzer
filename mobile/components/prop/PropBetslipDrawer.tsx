// components/prop/PropBetslipDrawer.tsx
import {
  View,
  Text,
  Pressable,
  StyleSheet,
  Linking,
  ScrollView,
} from "react-native";
import { useMemo, useState } from "react";
import * as Clipboard from "expo-clipboard";
import * as Haptics from "expo-haptics";

import { usePropBetslip } from "@/store/usePropBetslip";
import { useTheme } from "@/store/useTheme";

const GAMBLY_URL = "https://www.gambly.com/gambly-bot";

export function PropBetslipDrawer() {
  const { colors } = useTheme();
  const { items, remove, clear } = usePropBetslip();

  const [expanded, setExpanded] = useState(false);

  const text = useMemo(
    () =>
      items
        .map(
          (b) =>
            `${b.player} ${b.market} ${
              b.side === "under" ? "U" : "O"
            } ${b.line} ${b.odds}`
        )
        .join("\n"),
    [items]
  );

  if (!items.length) return null;

  return (
    <View
      style={[
        styles.wrap,
        {
          backgroundColor: colors.surface.card,
          borderTopColor: colors.border.subtle,
        },
      ]}
    >
      {/* HEADER */}
      <Pressable
        onPress={() => setExpanded((v) => !v)}
        style={styles.header}
      >
        <Text
          style={[
            styles.title,
            { color: colors.text.primary },
          ]}
        >
          Betslip ({items.length})
        </Text>

        <Text
          style={[
            styles.chevron,
            { color: colors.text.muted },
          ]}
        >
          {expanded ? "▼" : "▲"}
        </Text>
      </Pressable>

      {/* BET LIST */}
      {expanded && (
        <View style={{ maxHeight: 220 }}>
          <ScrollView
            showsVerticalScrollIndicator={false}
          >
            {items.map((b) => (
              <View
                key={b.id}
                style={[
                  styles.row,
                  {
                    borderBottomColor:
                      colors.border.subtle,
                  },
                ]}
              >
                <Text
                  style={[
                    styles.label,
                    { color: colors.text.primary },
                  ]}
                >
                  {b.player} {b.market}{" "}
                  {b.side === "under" ? "U" : "O"}{" "}
                  {b.line}
                </Text>

                <Text
                  style={[
                    styles.odds,
                    { color: colors.text.secondary },
                  ]}
                >
                  {b.odds}
                </Text>

                <Pressable
                  onPress={() => remove(b.id)}
                >
                  <Text
                    style={[
                      styles.remove,
                      { color: colors.text.muted },
                    ]}
                  >
                    ✕
                  </Text>
                </Pressable>
              </View>
            ))}
          </ScrollView>
        </View>
      )}

      {/* ACTIONS */}
      <View style={styles.actions}>
        <Pressable
          onPress={clear}
          style={[
            styles.clearBtn,
            {
              borderColor: colors.border.subtle,
            },
          ]}
        >
          <Text
            style={{ color: colors.text.muted }}
          >
            Clear All
          </Text>
        </Pressable>

        <Pressable
          onPress={async () => {
            await Clipboard.setStringAsync(text);
            Haptics.notificationAsync(
              Haptics.NotificationFeedbackType.Success
            );
          }}
          style={[
            styles.btn,
            {
              backgroundColor:
                colors.surface.elevated,
            },
          ]}
        >
          <Text
            style={[
              styles.btnText,
              { color: colors.text.primary },
            ]}
          >
            Copy Bets
          </Text>
        </Pressable>

        <Pressable
          onPress={() =>
            Linking.openURL(GAMBLY_URL)
          }
          style={[
            styles.btn,
            {
              backgroundColor:
                colors.accent.primary,
            },
          ]}
        >
          <Text
            style={[
              styles.btnText,
              { color: colors.text.inverse },
            ]}
          >
            Open Gambly
          </Text>
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
    zIndex: 1000,
    elevation: 20,
  },

  header: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    marginBottom: 8,
  },

  title: {
    fontSize: 16,
    fontWeight: "900",
  },

  chevron: {
    fontSize: 14,
    fontWeight: "800",
  },

  row: {
    flexDirection: "row",
    alignItems: "center",
    paddingVertical: 8,
    borderBottomWidth: 1,
  },

  label: {
    flex: 1,
    fontWeight: "700",
  },

  odds: {
    width: 52,
    textAlign: "right",
    fontWeight: "800",
  },

  remove: {
    marginLeft: 10,
    fontWeight: "900",
  },

  actions: {
    flexDirection: "row",
    gap: 8,
    marginTop: 10,
  },

  btn: {
    flex: 1,
    paddingVertical: 12,
    borderRadius: 14,
    alignItems: "center",
  },

  clearBtn: {
    paddingHorizontal: 12,
    justifyContent: "center",
    borderWidth: 1,
    borderRadius: 14,
  },

  btnText: {
    fontWeight: "900",
  },
});
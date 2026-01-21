// components/live/BetslipDrawer.tsx
import { View, Text, Pressable, StyleSheet, Animated } from "react-native";
import { useEffect, useRef } from "react";
import { useTheme } from "@/store/useTheme";
import { useSavedBets, SavedBet } from "@/store/useSavedBets";
import * as Clipboard from "expo-clipboard";

const DRAWER_HEIGHT = 320;

/* ============================
   LABEL FORMATTERS (ADJUSTMENT)
============================ */

function formatBetTitle(bet: SavedBet) {
  if (bet.betType === "player") {
    return `${bet.player} · ${bet.market}`;
  }
  return bet.teams;
}

function formatBetSubline(bet: SavedBet) {
  const odds =
    bet.odds != null ? ` (${bet.odds > 0 ? "+" : ""}${bet.odds})` : "";

  if (bet.betType === "game") {
    return `${bet.side.toUpperCase()} ${bet.line}${odds} · ${bet.bookmaker}`;
  }

  if (bet.side === "milestone") {
    return `${bet.line}+${odds} · ${bet.bookmaker}`;
  }

  return `${bet.side.toUpperCase()} ${bet.line}${odds} · ${bet.bookmaker}`;
}

/* ============================
   COMPONENT
============================ */

export function BetslipDrawer({
  open,
  onClose,
}: {
  open: boolean;
  onClose: () => void;
}) {
  const { colors } = useTheme();
  const betsMap = useSavedBets((s) => s.bets);
  const removeBet = useSavedBets((s) => s.toggleSave);
  const clearAll = useSavedBets((s) => s.clearAll);

  const bets = Array.from(betsMap.values());
  const betCount = bets.length;

  const translateY = useRef(
    new Animated.Value(DRAWER_HEIGHT)
  ).current;

  useEffect(() => {
    Animated.spring(translateY, {
      toValue: open ? 0 : DRAWER_HEIGHT,
      useNativeDriver: true,
      friction: 9,
    }).start();
  }, [open]);

  if (betCount === 0) return null;

  const copyAll = async () => {
    const text = bets
      .map((b) => `${formatBetTitle(b)}\n${formatBetSubline(b)}`)
      .join("\n\n");

    await Clipboard.setStringAsync(text);
  };

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
      {/* =====================
          HEADER
      ===================== */}
      <View style={styles.header}>
        <Text style={[styles.title, { color: colors.text.primary }]}>
          Betslip ({betCount})
        </Text>

        <Pressable onPress={onClose} hitSlop={8}>
          <Text style={{ color: colors.text.muted, fontWeight: "600" }}>
            Close
          </Text>
        </Pressable>
      </View>

      {/* =====================
          BET ROWS
      ===================== */}
      <View style={styles.body}>
        {bets.map((bet) => (
          <View key={bet.id} style={styles.betRow}>
            <View style={{ flex: 1 }}>
              <Text
                style={[
                  styles.betTitle,
                  { color: colors.text.primary },
                ]}
              >
                {formatBetTitle(bet)}
              </Text>

              <Text
                style={[
                  styles.betSub,
                  { color: colors.text.muted },
                ]}
              >
                {formatBetSubline(bet)}
              </Text>
            </View>

            <Pressable
              onPress={() => removeBet(bet)}
              hitSlop={8}
            >
              <Text
                style={{
                  color: colors.accent.danger,
                  fontWeight: "700",
                }}
              >
                ✕
              </Text>
            </Pressable>
          </View>
        ))}
      </View>

      {/* =====================
          ACTIONS
      ===================== */}
      <View style={styles.actions}>
        <Pressable
          onPress={copyAll}
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
            Copy Betslip
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

/* ============================
   STYLES (UNCHANGED)
============================ */

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
    elevation: 14,
  },

  header: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    marginBottom: 10,
  },

  title: {
    fontSize: 16,
    fontWeight: "800",
  },

  body: {
    flex: 1,
    gap: 10,
  },

  betRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 10,
  },

  betTitle: {
    fontSize: 14,
    fontWeight: "700",
  },

  betSub: {
    fontSize: 11,
    marginTop: 2,
  },

  actions: {
    gap: 10,
    marginTop: 8,
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
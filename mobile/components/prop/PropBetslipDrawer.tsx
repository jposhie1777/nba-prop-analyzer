// components/prop/PropBetslipDrawer.tsx
import {
  View,
  Text,
  Pressable,
  StyleSheet,
  Linking,
  ScrollView,
} from "react-native";
import { useMemo, useState, useEffect } from "react";
import * as Clipboard from "expo-clipboard";
import * as Haptics from "expo-haptics";

import { useBetslipDrawer } from "@/store/useBetslipDrawer";
import { useTheme } from "@/store/useTheme";
import { usePropBetslip } from "@/store/usePropBetslip";
import { useParlayTracker } from "@/store/useParlayTracker";
import { normalizeMarket } from "@/utils/normalizeMarket";

const GAMBLY_URL = "https://www.gambly.com/gambly-bot";
const STAKE = 10;

/* ======================================================
   ODDS HELPERS
====================================================== */
function americanToDecimal(odds: number) {
  return odds > 0
    ? 1 + odds / 100
    : 1 + 100 / Math.abs(odds);
}

function decimalToAmerican(decimal: number) {
  if (decimal >= 2) {
    return Math.round((decimal - 1) * 100);
  }
  return Math.round(-100 / (decimal - 1));
}

/* ======================================================
   COMPONENT
====================================================== */
export function PropBetslipDrawer() {
  const { colors } = useTheme();
  const { items, remove, clear } = usePropBetslip();

  const [expanded, setExpanded] = useState(false);
  const { isOpen, close, toggle } = useBetslipDrawer();
  const [shouldTrack, setShouldTrack] = useState(false);

  /* =========================
     AUTO COLLAPSE > 3 ITEMS
  ========================= */
  useEffect(() => {
    if (items.length > 3) {
      setExpanded(false);
    }
  }, [items.length]);

  useEffect(() => {
    if (!items.length) setShouldTrack(false);
  }, [items.length]);


  /* =========================
     COPY TEXT
  ========================= */
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

  /* =========================
     PARLAY ODDS
  ========================= */
  const parlayOdds = useMemo(() => {
    if (items.length < 2) return null;

    const decimal = items.reduce(
      (acc, b) => acc * americanToDecimal(b.odds),
      1
    );

    return decimalToAmerican(decimal);
  }, [items]);

  const { track } = useParlayTracker();

 /* =========================
    $10 PAYOUT
  ========================= */
  const payout = useMemo(() => {
    if (parlayOdds == null) return null;

    return parlayOdds > 0
      ? STAKE + (STAKE * parlayOdds) / 100
      : STAKE + (STAKE * 100) / Math.abs(parlayOdds);
  }, [parlayOdds]);

  /* =========================
    SNAPSHOT BUILDER
  ========================= */
  function buildSnapshot(
    source: "toggle" | "copy" | "gambly"
  ) {
    const legIds = items.map((b) => b.id).join("-");

    return {
      parlay_id: `${Date.now()}-${legIds}`,
      created_at: new Date().toISOString(),
      source,

      stake: STAKE,
      parlay_odds: parlayOdds,
      payout,

      legs: items.map((b) => {
        // ✅ normalize YES → OVER
        const sideNorm: "over" | "under" =
          b.side === "under" ? "under" : "over";
      
        return {
          leg_id: b.id,
          player_id: b.player_id,
          player_name: b.player,
      
          market: normalizeMarket(b.market),
          side: sideNorm, // 🔥 THIS IS THE FIX
          line: b.line,
          odds: b.odds,
        };
      }),
    };
  }

  /* =========================
    EARLY EXIT
  ========================= */
  if (!items.length && !isOpen) return null;


  return (
    <View
      style={[
        styles.wrap,
        {
          backgroundColor: colors.surface.card,
          borderTopColor: colors.border.subtle,
          transform: [{ translateY: isOpen ? 0 : 260 }],
        },
      ]}
    >
      {/* =========================
          HEADER
      ========================== */}
      <Pressable
        onPress={() => setExpanded((v) => !v)}
        style={styles.header}
      >
        <View>
          <Text
            style={[
              styles.title,
              { color: colors.text.primary },
            ]}
          >
            Betslip ({items.length})
          </Text>

          {parlayOdds != null && (
            <Text
              style={{
                fontSize: 12,
                fontWeight: "700",
                color: colors.text.muted,
                marginTop: 2,
              }}
            >
              Parlay Odds:{" "}
              {parlayOdds > 0
                ? `+${parlayOdds}`
                : parlayOdds}
              {payout && (
                <Text
                  style={{
                    color: colors.text.secondary,
                  }}
                >
                  {"  "}• ${STAKE} → $
                  {payout.toFixed(2)}
                </Text>
              )}
            </Text>
          )}
        </View>

        <Text
          style={[
            styles.chevron,
            { color: colors.text.muted },
          ]}
        >
          {expanded ? "▼" : "▲"}
        </Text>
      </Pressable>

      {/* =========================
          BET LIST
      ========================== */}
      {expanded && (
        <View style={{ maxHeight: 220 }}>
          <ScrollView showsVerticalScrollIndicator={false}>
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
                  onPress={() => {
                    remove(b.id);
                    if (items.length === 1) {
                      close();
                    }
                  }}
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

      <View style={{ marginTop: 6 }}>
        <Pressable
          onPress={() => setShouldTrack((v) => !v)}
          style={{
            flexDirection: "row",
            alignItems: "center",
            gap: 8,
          }}
        >
          <Text style={{ color: colors.text.primary, fontWeight: "800" }}>
            {shouldTrack ? "☑" : "☐"} Track this bet
          </Text>
        </Pressable>
      </View>

      {/* =========================
          ACTIONS
      ========================== */}
      <View style={styles.actions}>
        <Pressable
          onPress={() => {
            if (shouldTrack && items.length) {
              track(buildSnapshot("toggle"));
            }
            clear();
            close();
          }}

          style={[
            styles.clearBtn,
            { borderColor: colors.border.subtle },
          ]}
        >
          <Text style={{ color: colors.text.muted }}>
            Clear All
          </Text>
        </Pressable>

        <Pressable
          onPress={async () => {
            const snapshot = buildSnapshot("copy");
            track(snapshot);

            await Clipboard.setStringAsync(text);
            Haptics.notificationAsync(
              Haptics.NotificationFeedbackType.Success
            );
          }}

          style={[
            styles.btn,
            { backgroundColor: colors.surface.elevated },
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
          onPress={() => {
            const snapshot = buildSnapshot("gambly");
            track(snapshot);
            Linking.openURL(GAMBLY_URL);
          }}
          style={[
            styles.btn,
            { backgroundColor: colors.accent.primary },
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

/* ======================================================
   STYLES
====================================================== */
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
import { View, Text, StyleSheet, Pressable } from "react-native";
import { useTheme } from "@/store/useTheme";
import { useState, useCallback } from "react";
import { Swipeable } from "react-native-gesture-handler";
import * as Haptics from "expo-haptics";

import { usePropBetslip } from "@/store/usePropBetslip";
import { useBetslipDrawer } from "@/store/useBetslipDrawer";

/* ---------------------------------
   Pace helpers
---------------------------------- */
function parseClock(clock?: string): number | null {
  if (!clock) return null;
  const [m, s] = clock.split(":").map(Number);
  if (Number.isNaN(m) || Number.isNaN(s)) return null;
  return m + s / 60;
}

function getElapsedMinutes(
  period?: string,
  clock?: string,
  gameState?: string
): number | null {
  if (gameState === "halftime") return 24;

  const quarterIndex: Record<string, number> = {
    Q1: 0,
    Q2: 1,
    Q3: 2,
    Q4: 3,
  };

  if (!period || !(period in quarterIndex)) return null;

  const remaining = parseClock(clock);
  if (remaining == null) return null;

  return quarterIndex[period] * 12 + (12 - remaining);
}

function formatOdds(odds?: number) {
  if (odds == null) return "";
  return odds > 0 ? `+${odds}` : `${odds}`;
}

/* =====================================================
   CARD
===================================================== */
export default function LivePropCard({ item }: { item: any }) {
  const { colors } = useTheme();
  const [expanded, setExpanded] = useState(false);

  const add = usePropBetslip((s) => s.add);
  const openDrawer = useBetslipDrawer((s) => s.open);

  /* -----------------------------
     CONTEXT
  ------------------------------ */
  const contextText =
    item.game_state === "halftime"
      ? "Halftime"
      : item.game_period && item.game_clock
      ? `${item.away_team_abbr ?? "AWY"} vs ${
          item.home_team_abbr ?? "HOM"
        } · ${item.game_period} ${item.game_clock}`
      : `${item.away_team_abbr ?? "AWY"} vs ${
          item.home_team_abbr ?? "HOM"
        }`;

  /* -----------------------------
     PACE
  ------------------------------ */
  const elapsedMinutes = getElapsedMinutes(
    item.game_period,
    item.game_clock,
    item.game_state
  );

  const progress =
    elapsedMinutes && elapsedMinutes > 0
      ? elapsedMinutes / 48
      : null;

  const projectedFinal =
    progress && progress > 0.05
      ? item.current_stat / progress
      : null;

  /* -----------------------------
     BLOWOUT
  ------------------------------ */
  let blowoutLabel: string | null = null;
  let blowoutStyle = styles.pillWarning;

  if (item.score_margin >= 16) {
    blowoutLabel = "Blowout Risk";
    blowoutStyle = styles.pillDanger;
  } else if (item.score_margin >= 10) {
    blowoutLabel = "Blowout Watch";
  }

  /* -----------------------------
     SAVE HANDLER
  ------------------------------ */
  const handleSave = useCallback(() => {
    add({
      id: `live-${item.game_id}-${item.player_id}-${item.market}-${item.line}`,
      player_id: item.player_id,
      player: item.player_name,
      market: item.market,
      side: "over",
      line: item.line,
      odds: item.over_odds,
      matchup: `${item.away_team_abbr} @ ${item.home_team_abbr}`,
    });

    openDrawer();

    Haptics.notificationAsync(
      Haptics.NotificationFeedbackType.Success
    );
  }, [add, openDrawer, item]);

  /* -----------------------------
     SWIPE ACTION
  ------------------------------ */
  const renderSaveAction = () => (
    <View style={styles.swipeSave}>
      <Text style={styles.swipeSaveText}>Save</Text>
    </View>
  );

  return (
    <Swipeable
      renderRightActions={renderSaveAction}
      onSwipeableOpen={(direction) => {
        if (direction === "right") handleSave();
      }}
      overshootRight={false}
    >
      <Pressable
        onPress={() => setExpanded((v) => !v)}
        style={[
          styles.card,
          {
            backgroundColor: colors.surface.card,
            shadowColor: "#000",
          },
        ]}
      >
        {/* =========================
            HEADER
        ========================== */}
        <View style={styles.headerRow}>
          {/* Left */}
          <View style={styles.headerLeft}>
            <View
              style={[
                styles.headshot,
                { backgroundColor: colors.surface.cardSoft },
              ]}
            />

            <View>
              <Text style={[styles.player, { color: colors.text.primary }]}>
                {item.player_name ?? "Unknown Player"}
              </Text>

              <Text style={[styles.subtle, { color: colors.text.muted }]}>
                {contextText}
              </Text>
            </View>
          </View>

          {/* Right: Book + Odds */}
          <View style={styles.headerRight}>
            <View
              style={[
                styles.bookPill,
                { backgroundColor: colors.surface.elevated },
              ]}
            >
              <Text style={styles.bookText}>
                {item.book === "fanduel"
                  ? "FD"
                  : item.book === "draftkings"
                  ? "DK"
                  : "BK"}
              </Text>
            </View>

            {item.over_odds != null && (
              <Text style={[styles.odds, { color: colors.text.primary }]}>
                {formatOdds(item.over_odds)}
              </Text>
            )}
          </View>
        </View>

        {/* =========================
            MARKET
        ========================== */}
        <View style={styles.marketRow}>
          <Text style={[styles.title, { color: colors.text.primary }]}>
            {item.market.toUpperCase()} · {item.line}
          </Text>

          {blowoutLabel && (
            <View style={[styles.pill, blowoutStyle]}>
              <Text style={styles.pillText}>{blowoutLabel}</Text>
            </View>
          )}
        </View>

        <Text style={[styles.body, { color: colors.text.secondary }]}>
          Current {item.current_stat} → Need {item.remaining_needed}
        </Text>

        {/* =========================
            BOTTOM METRICS
        ========================== */}
        {projectedFinal && (
          <View style={styles.bottomRow}>
            <View style={[styles.pill, styles.pillNeutral]}>
              <Text style={styles.pillText}>
                Pace {projectedFinal.toFixed(1)}
              </Text>
            </View>
          </View>
        )}

        {/* =========================
            EXPANDED
        ========================== */}
        {expanded && (
          <View
            style={[
              styles.expanded,
              { borderColor: colors.border.subtle },
            ]}
          >
            <Text
              style={[
                styles.expandedHeader,
                { color: colors.text.primary },
              ]}
            >
              Current
            </Text>
            <View style={styles.expandedRow}>
              <Text style={styles.cell}>Projected: 28.4</Text>
              <Text style={styles.cell}>Minutes: 36.1</Text>
            </View>

            <Text
              style={[
                styles.expandedHeader,
                { color: colors.text.primary },
              ]}
            >
              Historical H2
            </Text>
            <View style={styles.expandedRow}>
              <Text style={styles.cell}>L5: 11.8</Text>
              <Text style={styles.cell}>L10: 10.9</Text>
              <Text style={styles.cell}>Min L5: 17.4</Text>
            </View>
          </View>
        )}
      </Pressable>
    </Swipeable>
  );
}

/* =====================================================
   STYLES
===================================================== */
const styles = StyleSheet.create({
  card: {
    padding: 16,
    borderRadius: 18,
    marginBottom: 14,
    shadowOpacity: 0.04,
    shadowRadius: 14,
    shadowOffset: { width: 0, height: 6 },
  },

  swipeSave: {
    flex: 1,
    backgroundColor: "#16A34A",
    justifyContent: "center",
    paddingLeft: 24,
    borderRadius: 18,
  },

  swipeSaveText: {
    color: "#fff",
    fontSize: 14,
    fontWeight: "900",
  },

  headerRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    gap: 12,
  },

  headerLeft: {
    flexDirection: "row",
    alignItems: "center",
    gap: 12,
    flex: 1,
  },

  headerRight: {
    alignItems: "flex-end",
    gap: 4,
  },

  headshot: {
    width: 44,
    height: 44,
    borderRadius: 22,
  },

  player: {
    fontSize: 16,
    fontWeight: "900",
  },

  subtle: {
    fontSize: 12,
    fontWeight: "600",
    marginTop: 2,
  },

  marketRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    marginTop: 12,
  },

  title: {
    fontSize: 15,
    fontWeight: "900",
  },

  body: {
    marginTop: 6,
    fontSize: 12,
    fontWeight: "600",
    opacity: 0.85,
  },

  bottomRow: {
    marginTop: 10,
    flexDirection: "row",
  },

  pill: {
    paddingHorizontal: 10,
    paddingVertical: 4,
    borderRadius: 999,
  },

  pillText: {
    fontSize: 11,
    fontWeight: "800",
    color: "#111",
  },

  pillNeutral: {
    backgroundColor: "#EEF2FF",
  },

  pillWarning: {
    backgroundColor: "#FEF3C7",
  },

  pillDanger: {
    backgroundColor: "#FEE2E2",
  },

  expanded: {
    marginTop: 14,
    paddingTop: 12,
    borderTopWidth: 1,
  },

  expandedHeader: {
    fontSize: 12,
    fontWeight: "900",
    marginBottom: 6,
  },

  expandedRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    marginBottom: 10,
  },

  cell: {
    fontSize: 12,
    fontWeight: "700",
  },

  bookPill: {
    minWidth: 34,
    height: 22,
    borderRadius: 7,
    alignItems: "center",
    justifyContent: "center",
    paddingHorizontal: 6,
  },

  bookText: {
    fontSize: 11,
    fontWeight: "900",
  },

  odds: {
    fontSize: 14,
    fontWeight: "900",
  },
});
// /components/live/LivePropCard.tsx
import {
  View,
  Text,
  StyleSheet,
  Pressable,
  Image,
} from "react-native";
import { useTheme } from "@/store/useTheme";
import { useState, useCallback, useRef } from "react";
import { Swipeable } from "react-native-gesture-handler";
import * as Haptics from "expo-haptics";

import { TEAM_LOGOS } from "@/utils/teamLogos";
import { usePropBetslip } from "@/store/usePropBetslip";
import { useBetslipDrawer } from "@/store/useBetslipDrawer";
import { InteractionManager } from "react-native";

/* =====================================================
   HELPERS
===================================================== */

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

function formatAverage(value?: number | null) {
  if (value == null || Number.isNaN(value)) return "—";
  return value.toFixed(1);
}

function formatLine(value?: number | null) {
  if (value == null || Number.isNaN(value)) return "—";
  return Number.isInteger(value) ? `${value}` : value.toFixed(1);
}

/* =====================================================
   CARD
===================================================== */

export default function LivePropCard({ item }: { item: any }) {
  const { colors } = useTheme();
  const [expanded, setExpanded] = useState(false);
  const swipeRef = useRef<Swipeable>(null);

  const add = usePropBetslip((s) => s.add);
  const openDrawer = useBetslipDrawer((s) => s.open);

  /* -----------------------------
     ODDS COLOR
  ------------------------------ */
  const oddsColor =
    item.display_odds_side === "OVER"
      ? "#16A34A"
      : item.display_odds_side === "UNDER"
      ? "#DC2626"
      : colors.text.muted;

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
      odds: item.display_odds,
      matchup: `${item.away_team_abbr} @ ${item.home_team_abbr}`,
    });
  
    // ✅ GUARANTEED to fire after swipe animation finishes
    InteractionManager.runAfterInteractions(() => {
      openDrawer();
    });
  
    Haptics.notificationAsync(
      Haptics.NotificationFeedbackType.Success
    );
  }, [add, openDrawer, item]);

  /* -----------------------------
     SWIPE ACTION (LEFT SIDE)
  ------------------------------ */
  const renderSaveAction = () => (
    <View style={styles.swipeSave}>
      <Text style={styles.swipeSaveText}>Save</Text>
    </View>
  );

  return (
    <Swipeable
      ref={swipeRef}
      renderLeftActions={renderSaveAction}
      onSwipeableOpen={(direction) => {
        if (direction === "left") {
          handleSave();
          swipeRef.current?.close();
        }
      }}
      overshootLeft={false}
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
            >
              {item.player_image_url ? (
                <Image
                  source={{ uri: item.player_image_url }}
                  style={styles.headshotImage}
                  resizeMode="cover"
                />
              ) : null}
            </View>

            <View>
              <Text
                style={[
                  styles.player,
                  { color: colors.text.primary },
                ]}
              >
                {item.player_name ?? "Unknown Player"}
              </Text>

              <View style={styles.matchupRow}>
                {item.away_team_abbr &&
                  TEAM_LOGOS[item.away_team_abbr] && (
                    <Image
                      source={{
                        uri: TEAM_LOGOS[item.away_team_abbr],
                      }}
                      style={styles.teamLogo}
                    />
                  )}

                <Text
                  style={[
                    styles.subtle,
                    { color: colors.text.muted },
                  ]}
                >
                  @
                </Text>

                {item.home_team_abbr &&
                  TEAM_LOGOS[item.home_team_abbr] && (
                    <Image
                      source={{
                        uri: TEAM_LOGOS[item.home_team_abbr],
                      }}
                      style={styles.teamLogo}
                    />
                  )}

                <Text
                  style={[
                    styles.subtle,
                    { color: colors.text.muted },
                  ]}
                >
                  {item.game_period ?? ""}
                  {item.game_clock
                    ? ` · ${item.game_clock}`
                    : ""}
                </Text>
              </View>
            </View>
          </View>

          {/* Right */}
          <View style={styles.headerRight}>
            <View
              style={[
                styles.bookPill,
                {
                  backgroundColor:
                    colors.surface.elevated,
                },
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

            {item.display_odds != null && (
              <View style={{ alignItems: "flex-end" }}>
                <Text
                  style={[
                    styles.odds,
                    { color: oddsColor },
                  ]}
                >
                  {formatOdds(item.display_odds)}
                </Text>

                {item.display_odds_side && (
                  <Text
                    style={[
                      styles.subtle,
                      { fontSize: 10 },
                    ]}
                  >
                    {item.display_odds_side}
                  </Text>
                )}
              </View>
            )}
          </View>
        </View>

        {/* =========================
            MARKET
        ========================== */}
        <View style={styles.marketRow}>
          <Text
            style={[
              styles.title,
              { color: colors.text.primary },
            ]}
          >
            {item.market.toUpperCase()} · {item.line}
          </Text>

          {blowoutLabel && (
            <View style={[styles.pill, blowoutStyle]}>
              <Text style={styles.pillText}>
                {blowoutLabel}
              </Text>
            </View>
          )}
        </View>

        <Text
          style={[
            styles.body,
            { color: colors.text.secondary },
          ]}
        >
          Current {item.current_stat} → Need{" "}
          {item.remaining_needed}
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
              Line Check
            </Text>

            <View style={styles.expandedRow}>
              <Text style={styles.cell}>
                Line: {formatLine(item.line)}
              </Text>
              <Text style={styles.cell}>
                Current: {formatLine(item.current_stat)}
              </Text>
            </View>

            <Text
              style={[
                styles.expandedHeader,
                { color: colors.text.primary },
              ]}
            >
              Historical Averages
            </Text>

            <View style={styles.expandedRow}>
              <Text style={styles.cell}>
                Q3: {formatAverage(item.avg_q3)}
              </Text>
              <Text style={styles.cell}>
                Q4: {formatAverage(item.avg_q4)}
              </Text>
              <Text style={styles.cell}>
                H2: {formatAverage(item.avg_h2)}
              </Text>
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

  headshotImage: {
    width: "100%",
    height: "100%",
    borderRadius: 22,
  },

  matchupRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 6,
    marginTop: 2,
  },

  teamLogo: {
    width: 16,
    height: 16,
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
